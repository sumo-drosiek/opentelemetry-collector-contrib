// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sumologicexporter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer/pdata"
)

// metricPair represents information required to send one metric to the Sumo Logic
type metricPair struct {
	attributes pdata.AttributeMap
	metric     pdata.Metric
}

type sender struct {
	buffer       []pdata.LogRecord
	metricBuffer []metricPair
	config       *Config
	client       *http.Client
	filter       filter
}

const (
	logKey string = "log"
	// maxBufferSize defines size of the buffer (maximum number of pdata.LogRecord entries)
	maxBufferSize int = 1024 * 1024
)

func newSender(cfg *Config, cl *http.Client, f filter) *sender {
	return &sender{
		config: cfg,
		client: cl,
		filter: f,
	}
}

// Send sends data to sumologic
func (s *sender) send(pipeline PipelineType, body io.Reader, fields Fields) error {
	// Add headers
	req, err := http.NewRequest(http.MethodPost, s.config.HTTPClientSettings.Endpoint, body)
	if err != nil {
		return err
	}

	req.Header.Add("X-Sumo-Client", s.config.Client)

	if len(s.config.SourceHost) > 0 {
		req.Header.Add("X-Sumo-Host", s.config.SourceHost)
	}

	if len(s.config.SourceName) > 0 {
		req.Header.Add("X-Sumo-Name", s.config.SourceName)
	}

	if len(s.config.SourceCategory) > 0 {
		req.Header.Add("X-Sumo-Category", s.config.SourceCategory)
	}

	switch pipeline {
	case LogsPipeline:
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("X-Sumo-Fields", string(fields))
	case MetricsPipeline:
		switch s.config.MetricFormat {
		case Carbon2Format:
			req.Header.Add("Content-Type", "application/vnd.sumologic.carbon2")
		default:
			// ToDo: Implement metrics pipeline
			return errors.New("current sender version doesn't support metrics")
		}
	default:
		return errors.New("unexpected pipeline")
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("error during sending data: %s", resp.Status)
	}
	return nil
}

// sendLogs sends logs in right format basing on the s.config.LogFormat
func (s *sender) sendLogs(fields Fields) ([]pdata.LogRecord, error) {
	switch s.config.LogFormat {
	case TextFormat:
		return s.sendLogsTextFormat(fields)
	case JSONFormat:
		return s.sendLogsJSONFormat(fields)
	default:
		return nil, errors.New("unexpected log format")
	}
}

func (s *sender) sendLogsTextFormat(fields Fields) ([]pdata.LogRecord, error) {
	var (
		body           strings.Builder
		errs           []error
		droppedRecords []pdata.LogRecord
		currentRecords []pdata.LogRecord
	)

	for _, record := range s.buffer {
		sent, appended, err := s.appendAndSend(record.Body().StringVal(), LogsPipeline, &body, fields)
		if err != nil {
			errs = append(errs, err)
			if sent {
				droppedRecords = append(droppedRecords, currentRecords...)
			}

			if !appended {
				droppedRecords = append(droppedRecords, record)
			}
		}

		// If data was sent, cleanup the currentTimeSeries counter
		if sent {
			currentRecords = currentRecords[:0]
		}

		// If log has been appended to body, increment the currentTimeSeries
		if appended {
			currentRecords = append(currentRecords, record)
		}
	}

	if err := s.send(LogsPipeline, strings.NewReader(body.String()), fields); err != nil {
		errs = append(errs, err)
		droppedRecords = append(droppedRecords, currentRecords...)
	}

	if len(errs) > 0 {
		return droppedRecords, componenterror.CombineErrors(errs)
	}
	return droppedRecords, nil
}

func (s *sender) sendLogsJSONFormat(fields Fields) ([]pdata.LogRecord, error) {
	var (
		body strings.Builder
		errs []error
		// droppedRecords tracks all dropped records
		droppedRecords []pdata.LogRecord
		// currentRecords tracks records which are being actually processed
		currentRecords []pdata.LogRecord
	)

	for _, record := range s.buffer {
		data := s.filter.filterOut(record.Attributes())
		data[logKey] = record.Body().StringVal()

		nextLine, err := json.Marshal(data)
		if err != nil {
			droppedRecords = append(droppedRecords, record)
			errs = append(errs, err)
			continue
		}

		sent, appended, err := s.appendAndSend(bytes.NewBuffer(nextLine).String(), LogsPipeline, &body, fields)
		if err != nil {
			errs = append(errs, err)
			if sent {
				droppedRecords = append(droppedRecords, currentRecords...)
			}

			if !appended {
				droppedRecords = append(droppedRecords, record)
			}
		}

		// If data was sent, cleanup the currentRecords
		if sent {
			currentRecords = currentRecords[:0]
		}

		// If log has been appended to body, add it to the currentRecords
		if appended {
			currentRecords = append(currentRecords, record)
		}
	}

	if err := s.send(LogsPipeline, strings.NewReader(body.String()), fields); err != nil {
		errs = append(errs, err)
		droppedRecords = append(droppedRecords, currentRecords...)
	}

	if len(errs) > 0 {
		return droppedRecords, componenterror.CombineErrors(errs)
	}
	return droppedRecords, nil
}

// appendAndSend appends line to the request body that will be sent and sends
// the accumulated data if the internal buffer has been filled (with maxBufferSize elements).
// It returns 2 booleans saying whether the data was sent and whether the provided data
// was appended and an error if anything failed.
func (s *sender) appendAndSend(
	line string,
	pipeline PipelineType,
	body *strings.Builder,
	fields Fields,
) (sent bool, appended bool, _ error) {
	var errors []error
	// sent gives information if the data was sent or not
	sent = false
	// appended keeps state of appending new log line to the body
	appended = true

	if body.Len() > 0 && body.Len()+len(line) > s.config.MaxRequestBodySize {
		sent = true
		if err := s.send(pipeline, strings.NewReader(body.String()), fields); err != nil {
			errors = append(errors, err)
		}
		body.Reset()
	}

	if body.Len() > 0 {
		// Do not add newline if the body is empty
		if _, err := body.WriteString("\n"); err != nil {
			errors = append(errors, err)
			appended = false
		}
	}

	if _, err := body.WriteString(line); err != nil && appended {
		errors = append(errors, err)
		appended = false
	}

	if len(errors) > 0 {
		return sent, appended, componenterror.CombineErrors(errors)
	}
	return sent, appended, nil
}

// cleanBuffer zeroes buffer
func (s *sender) cleanBuffer() {
	s.buffer = (s.buffer)[:0]
}

// batch adds log to the buffer and flushes them if buffer is full to avoid overflow
// returns list of log records which were not sent successfully
func (s *sender) batch(log pdata.LogRecord, metadata Fields) ([]pdata.LogRecord, error) {
	s.buffer = append(s.buffer, log)

	if s.count() == maxBufferSize {
		dropped, err := s.sendLogs(metadata)
		s.cleanBuffer()
		return dropped, err
	}

	return nil, nil
}

// count returns number of logs in buffer
func (s *sender) count() int {
	return len(s.buffer)
}

// count returns number of metrics in buffer
func (s *sender) countMetrics() int {
	return len(s.metricBuffer)
}

// sendMetrics sends metrics in right format basing on the s.config.MetricFormat
func (s *sender) sendMetrics() ([]metricPair, error) {
	switch s.config.MetricFormat {
	case Carbon2Format:
		return s.sendMetricsCarbon2Format()
	default:
		return nil, errors.New("unexpected metric format")
	}
}

// batchMetric adds metric to the metrics buffer and flushes them if buffer is full to avoid overflow
// returns list of metric records which were not sent successfully
func (s *sender) batchMetric(metric metricPair) ([]metricPair, error) {
	s.metricBuffer = append(s.metricBuffer, metric)

	if s.countMetrics() == maxBufferSize {
		dropped, err := s.sendMetrics()
		s.cleanBuffer()
		return dropped, err
	}

	return nil, nil
}

// Carbon2IntRecord converts IntDataPoint to carbon2 metric string
// with additional information from metricPair
func (s *sender) Carbon2IntRecord(record metricPair, dataPoint pdata.IntDataPoint) string {
	return fmt.Sprintf("%s  %d %d",
		s.filter.Carbon2TagString(record),
		dataPoint.Value(),
		dataPoint.Timestamp()/1e9,
	)
}

// Carbon2DoubleRecord converts DoubleDataPoint to carbon2 metric string
// with additional information from metricPair
func (s *sender) Carbon2DoubleRecord(record metricPair, dataPoint pdata.DoubleDataPoint) string {
	return fmt.Sprintf("%s  %g %d",
		s.filter.Carbon2TagString(record),
		dataPoint.Value(),
		dataPoint.Timestamp()/1e9,
	)
}

func (s *sender) sendMetricsCarbon2Format() ([]metricPair, error) {
	var (
		body strings.Builder
		errs []error
		// droppedRecords tracks all dropped records
		droppedRecords []metricPair
		// currentRecords tracks records which are being actually processed
		currentRecords []metricPair
		nextLines      []string
	)

	for _, record := range s.metricBuffer {
		switch record.metric.DataType() {
		case pdata.MetricDataTypeIntGauge:
			for i := 0; i < record.metric.IntGauge().DataPoints().Len(); i++ {
				dataPoint := record.metric.IntGauge().DataPoints().At(i)
				nextLines = append(nextLines, s.Carbon2IntRecord(record, dataPoint))
			}
		case pdata.MetricDataTypeIntSum:
			for i := 0; i < record.metric.IntSum().DataPoints().Len(); i++ {
				dataPoint := record.metric.IntSum().DataPoints().At(i)
				nextLines = append(nextLines, s.Carbon2IntRecord(record, dataPoint))
			}
		case pdata.MetricDataTypeDoubleGauge:
			for i := 0; i < record.metric.DoubleGauge().DataPoints().Len(); i++ {
				dataPoint := record.metric.DoubleGauge().DataPoints().At(i)
				nextLines = append(nextLines, s.Carbon2DoubleRecord(record, dataPoint))
			}
		case pdata.MetricDataTypeDoubleSum:
			for i := 0; i < record.metric.DoubleSum().DataPoints().Len(); i++ {
				dataPoint := record.metric.DoubleSum().DataPoints().At(i)
				nextLines = append(nextLines, s.Carbon2DoubleRecord(record, dataPoint))
			}
		// Skip histogram metrics
		case pdata.MetricDataTypeDoubleHistogram:
		case pdata.MetricDataTypeIntHistogram:
		default:
			// Nothing to do here
		}

		sent, appended, err := s.appendAndSend(strings.Join(nextLines, "\n"), MetricsPipeline, &body, "")
		if err != nil {
			errs = append(errs, err)
			if sent {
				droppedRecords = append(droppedRecords, currentRecords...)
			}

			if !appended {
				droppedRecords = append(droppedRecords, record)
			}
		}

		nextLines = nextLines[:0]

		// If data was sent, cleanup the currentRecords
		if sent {
			currentRecords = currentRecords[:0]
		}

		// If log has been appended to body, add it to the currentRecords
		if appended {
			currentRecords = append(currentRecords, record)
		}
	}

	if err := s.send(MetricsPipeline, strings.NewReader(body.String()), ""); err != nil {
		errs = append(errs, err)
		droppedRecords = append(droppedRecords, currentRecords...)
	}

	if len(errs) > 0 {
		return droppedRecords, componenterror.CombineErrors(errs)
	}
	return droppedRecords, nil
}
