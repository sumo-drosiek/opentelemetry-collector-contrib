// Copyright 2020 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sumologicexporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type sumologicexporter struct {
	config     *Config
	client     *http.Client
	filter     filter
	prometheus prometheusFormatter
}

func initExporter(cfg *Config) (*sumologicexporter, error) {
	switch cfg.LogFormat {
	case JSONFormat:
	case TextFormat:
	default:
		return nil, fmt.Errorf("unexpected log format: %s", cfg.LogFormat)
	}

	switch cfg.MetricFormat {
	case GraphiteFormat:
	case Carbon2Format:
	case PrometheusFormat:
	default:
		return nil, fmt.Errorf("unexpected metric format: %s", cfg.MetricFormat)
	}

	switch cfg.CompressEncoding {
	case GZIPCompression:
	case DeflateCompression:
	case NoCompression:
	default:
		return nil, fmt.Errorf("unexpected compression encoding: %s", cfg.CompressEncoding)
	}

	if len(cfg.HTTPClientSettings.Endpoint) == 0 {
		return nil, errors.New("endpoint is not set")
	}

	f, err := newFilter(cfg.MetadataAttributes)
	if err != nil {
		return nil, err
	}

	httpClient, err := cfg.HTTPClientSettings.ToClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP Client: %w", err)
	}

	pf, err := newPrometheusFormatter()
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus formatter: %w", err)
	}

	se := &sumologicexporter{
		config:     cfg,
		client:     httpClient,
		filter:     f,
		prometheus: pf,
	}

	return se, nil
}

func newLogsExporter(
	cfg *Config,
	params component.ExporterCreateParams,
) (component.LogsExporter, error) {
	se, err := initExporter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize the logs exporter: %w", err)
	}

	return exporterhelper.NewLogsExporter(
		cfg,
		params.Logger,
		se.pushLogsData,
		// Disable exporterhelper Timeout, since we are using a custom mechanism
		// within exporter itself
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(cfg.RetrySettings),
		exporterhelper.WithQueue(cfg.QueueSettings),
	)
}

// pushLogsData groups data with common metadata and sends them as separate batched requests.
// It returns the number of unsent logs and error which contains list of dropped records
// so they can be handled by OTC retry mechanism
func (se *sumologicexporter) pushLogsData(_ context.Context, ld pdata.Logs) (int, error) {
	var (
		currentMetadata  Fields
		previousMetadata Fields
		errors           []error
		sdr              = newSender(se.config, se.client, se.filter, se.prometheus)
		droppedRecords   []pdata.LogRecord
	)

	// Iterate over ResourceLogs
	rl := ld.ResourceLogs()
	for i := 0; i < rl.Len(); i++ {
		resource := rl.At(i)

		ill := resource.InstrumentationLibraryLogs()
		// iterate over InstrumentationLibraryLogs
		for j := 0; j < ill.Len(); j++ {
			library := ill.At(j)

			// iterate over Logs
			ll := library.Logs()
			for k := 0; k < ll.Len(); k++ {
				log := ll.At(k)
				currentMetadata = sdr.filter.GetMetadata(log.Attributes())

				// If metadata differs from currently buffered, flush the buffer
				if currentMetadata != previousMetadata && previousMetadata != "" {
					dropped, err := sdr.sendLogs(previousMetadata)
					if err != nil {
						droppedRecords = append(droppedRecords, dropped...)
						errors = append(errors, err)
					}
					sdr.cleanBuffer()
				}

				// assign metadata
				previousMetadata = currentMetadata

				// add log to the buffer
				dropped, err := sdr.batch(log, previousMetadata)
				if err != nil {
					droppedRecords = append(droppedRecords, dropped...)
					errors = append(errors, err)
				}
			}
		}
	}

	// Flush pending logs
	dropped, err := sdr.sendLogs(previousMetadata)
	if err != nil {
		droppedRecords = append(droppedRecords, dropped...)
		errors = append(errors, err)
	}

	if len(droppedRecords) > 0 {
		// Move all dropped records to Logs
		droppedLogs := pdata.NewLogs()
		rss := droppedLogs.ResourceLogs()
		rss.Resize(1)

		lib := rss.At(0).InstrumentationLibraryLogs()
		lib.Resize(1)
		llogs := lib.At(0).Logs()

		for _, record := range droppedRecords {
			llogs.Append(record)
		}

		return len(droppedRecords), consumererror.PartialLogsError(componenterror.CombineErrors(errors), droppedLogs)
	}

	return 0, nil
}

func newMetricsExporter(
	cfg *Config,
	params component.ExporterCreateParams,
) (component.MetricsExporter, error) {
	se, err := initExporter(cfg)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		cfg,
		params.Logger,
		se.pushMetricsData,
		// Disable exporterhelper Timeout, since we are using a custom mechanism
		// within exporter itself
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(cfg.RetrySettings),
		exporterhelper.WithQueue(cfg.QueueSettings),
	)
}

// pushMetricsData groups data with common metadata and send them as separate batched requests
// it returns number of unsent metrics and error which contains list of dropped records
// so they can be handle by the OTC retry mechanism
func (se *sumologicexporter) pushMetricsData(_ context.Context, ld pdata.Metrics) (int, error) {
	var (
		errors         []error
		sdr            = newSender(se.config, se.client, se.filter, se.prometheus)
		droppedRecords []metricPair
		attributes     pdata.AttributeMap
	)

	// Iterate over ResourceMetrics
	rms := ld.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)

		if rm.IsNil() {
			continue
		}

		attributes = rm.Resource().Attributes()

		// iterate over InstrumentationLibraryMetrics
		ilms := rm.InstrumentationLibraryMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ilm := ilms.At(j)
			if ilm.IsNil() {
				continue
			}

			// iterate over Metrics
			ms := ilm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.IsNil() {
					continue
				}
				mp := metricPair{
					metric:     m,
					attributes: attributes,
				}
				// add metric to the buffer
				dropped, err := sdr.batchMetric(mp)
				if err != nil {
					droppedRecords = append(droppedRecords, dropped...)
					errors = append(errors, err)
				}
			}
		}
	}

	// Flush pending metrics
	dropped, err := sdr.sendMetrics()
	if err != nil {
		droppedRecords = append(droppedRecords, dropped...)
		errors = append(errors, err)
	}

	if len(droppedRecords) > 0 {
		// Move all dropped records to Metrics
		droppedMetrics := pdata.NewMetrics()
		rms := droppedMetrics.ResourceMetrics()
		rms.Resize(len(droppedRecords))
		for num, record := range droppedRecords {
			rm := droppedMetrics.ResourceMetrics().At(num)
			rm.Resource().InitEmpty()
			record.attributes.CopyTo(rm.Resource().Attributes())

			ilms := rm.InstrumentationLibraryMetrics()
			ilms.Resize(1)
			ilms.At(0).Metrics().Append(record.metric)
		}

		return len(droppedRecords), consumererror.PartialMetricsError(componenterror.CombineErrors(errors), droppedMetrics)
	}

	return 0, nil
}
