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
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/pdata"
)

func LogRecordsToLogs(records []pdata.LogRecord) pdata.Logs {
	logs := pdata.NewLogs()
	logs.ResourceLogs().Resize(1)
	logs.ResourceLogs().At(0).InstrumentationLibraryLogs().Resize(1)
	for _, record := range records {
		logs.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).Logs().Append(record)
	}

	return logs
}

func TestInitExporter(t *testing.T) {
	_, err := initExporter(&Config{
		LogFormat:        "json",
		MetricFormat:     "carbon2",
		CompressEncoding: "gzip",
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout:  defaultTimeout,
			Endpoint: "test_endpoint",
		},
	})
	assert.NoError(t, err)
}

func TestInitExporterInvalidLogFormat(t *testing.T) {
	_, err := initExporter(&Config{
		LogFormat:        "test_format",
		MetricFormat:     "carbon2",
		CompressEncoding: "gzip",
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout:  defaultTimeout,
			Endpoint: "test_endpoint",
		},
	})

	assert.EqualError(t, err, "unexpected log format: test_format")
}

func TestInitExporterInvalidMetricFormat(t *testing.T) {
	_, err := initExporter(&Config{
		LogFormat:    "json",
		MetricFormat: "test_format",
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout:  defaultTimeout,
			Endpoint: "test_endpoint",
		},
		CompressEncoding: "gzip",
	})

	assert.EqualError(t, err, "unexpected metric format: test_format")
}

func TestInitExporterInvalidCompressEncoding(t *testing.T) {
	_, err := initExporter(&Config{
		LogFormat:        "json",
		MetricFormat:     "carbon2",
		CompressEncoding: "test_format",
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout:  defaultTimeout,
			Endpoint: "test_endpoint",
		},
	})

	assert.EqualError(t, err, "unexpected compression encoding: test_format")
}

func TestInitExporterInvalidEndpoint(t *testing.T) {
	_, err := initExporter(&Config{
		LogFormat:        "json",
		MetricFormat:     "carbon2",
		CompressEncoding: "gzip",
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Timeout: defaultTimeout,
		},
	})

	assert.EqualError(t, err, "endpoint is not set")
}

func TestAllLogsSuccess(t *testing.T) {
	test := prepareSenderTest(t, []func(w http.ResponseWriter, req *http.Request){
		func(w http.ResponseWriter, req *http.Request) {
			body := extractBody(t, req)
			assert.Equal(t, `Example log`, body)
			assert.Equal(t, "", req.Header.Get("X-Sumo-Fields"))
		},
	})
	defer func() { test.srv.Close() }()

	logs := LogRecordsToLogs(exampleLog())

	_, err := test.exp.pushLogsData(context.Background(), logs)
	assert.NoError(t, err)
}

func TestAllLogsFailed(t *testing.T) {
	test := prepareSenderTest(t, []func(w http.ResponseWriter, req *http.Request){
		func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)

			body := extractBody(t, req)
			assert.Equal(t, "Example log\nAnother example log", body)
			assert.Equal(t, "", req.Header.Get("X-Sumo-Fields"))
		},
	})
	defer func() { test.srv.Close() }()

	logs := LogRecordsToLogs(exampleTwoLogs())

	dropped, err := test.exp.pushLogsData(context.Background(), logs)
	assert.EqualError(t, err, "error during sending data: 500 Internal Server Error")
	assert.Equal(t, 2, dropped)

	partial, ok := err.(consumererror.PartialError)
	require.True(t, ok)
	assert.Equal(t, logs, partial.GetLogs())
}

func TestLogsPartiallyFailed(t *testing.T) {
	test := prepareSenderTest(t, []func(w http.ResponseWriter, req *http.Request){
		func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)

			body := extractBody(t, req)
			assert.Equal(t, "Example log", body)
			assert.Equal(t, "key1=value1, key2=value2", req.Header.Get("X-Sumo-Fields"))
		},
		func(w http.ResponseWriter, req *http.Request) {
			body := extractBody(t, req)
			assert.Equal(t, "Another example log", body)
			assert.Equal(t, "key3=value3, key4=value4", req.Header.Get("X-Sumo-Fields"))
		},
	})
	defer func() { test.srv.Close() }()

	f, err := newFilter([]string{`key\d`})
	require.NoError(t, err)
	test.exp.filter = f

	records := exampleTwoDifferentLogs()
	logs := LogRecordsToLogs(records)
	expected := LogRecordsToLogs(records[:1])

	dropped, err := test.exp.pushLogsData(context.Background(), logs)
	assert.EqualError(t, err, "error during sending data: 500 Internal Server Error")
	assert.Equal(t, 1, dropped)

	partial, ok := err.(consumererror.PartialError)
	require.True(t, ok)
	assert.Equal(t, expected, partial.GetLogs())
}

func metricPairToMetrics(mp []metricPair) pdata.Metrics {
	metrics := pdata.NewMetrics()
	metrics.ResourceMetrics().Resize(len(mp))
	for num, record := range mp {
		metrics.ResourceMetrics().At(num).Resource().InitEmpty()
		record.attributes.CopyTo(metrics.ResourceMetrics().At(num).Resource().Attributes())
		metrics.ResourceMetrics().At(num).InstrumentationLibraryMetrics().Resize(1)
		metrics.ResourceMetrics().At(num).InstrumentationLibraryMetrics().At(0).Metrics().Append(record.metric)
	}

	return metrics
}

func TestAllMetricsSuccess(t *testing.T) {
	test := prepareSenderTest(t, []func(w http.ResponseWriter, req *http.Request){
		func(w http.ResponseWriter, req *http.Request) {
			body := extractBody(t, req)
			expected := `test=test_value test2=second_value metric=test.metric.data unit=bytes  14500 1605534165
another_test=test_value metric=test.metric.data2 unit=s  123 1605534144
another_test=test_value metric=test.metric.data2 unit=s  124 1605534145`
			assert.Equal(t, expected, body)
			assert.Equal(t, "application/vnd.sumologic.carbon2", req.Header.Get("Content-Type"))
		},
	})
	defer func() { test.srv.Close() }()
	metrics := metricPairToMetrics(exampleTwoIntMetrics())

	_, err := test.exp.pushMetricsData(context.Background(), metrics)
	assert.NoError(t, err)
}

func TestAllMetricsFailed(t *testing.T) {
	test := prepareSenderTest(t, []func(w http.ResponseWriter, req *http.Request){
		func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)

			body := extractBody(t, req)
			expected := `test=test_value test2=second_value metric=test.metric.data unit=bytes  14500 1605534165
another_test=test_value metric=test.metric.data2 unit=s  123 1605534144
another_test=test_value metric=test.metric.data2 unit=s  124 1605534145`
			assert.Equal(t, expected, body)
			assert.Equal(t, "application/vnd.sumologic.carbon2", req.Header.Get("Content-Type"))
		},
	})
	defer func() { test.srv.Close() }()
	metrics := metricPairToMetrics(exampleTwoIntMetrics())

	dropped, err := test.exp.pushMetricsData(context.Background(), metrics)
	assert.EqualError(t, err, "error during sending data: 500 Internal Server Error")
	assert.Equal(t, 2, dropped)

	partial, ok := err.(consumererror.PartialError)
	require.True(t, ok)
	assert.Equal(t, metrics, partial.GetMetrics())
}

func TestMetricsPartiallyFailed(t *testing.T) {
	test := prepareSenderTest(t, []func(w http.ResponseWriter, req *http.Request){
		func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(500)

			body := extractBody(t, req)
			expected := "test=test_value test2=second_value metric=test.metric.data unit=bytes  14500 1605534165"
			assert.Equal(t, expected, body)
			assert.Equal(t, "application/vnd.sumologic.carbon2", req.Header.Get("Content-Type"))
		},
		func(w http.ResponseWriter, req *http.Request) {
			body := extractBody(t, req)
			expected := `another_test=test_value metric=test.metric.data2 unit=s  123 1605534144
another_test=test_value metric=test.metric.data2 unit=s  124 1605534145`
			assert.Equal(t, expected, body)
			assert.Equal(t, "application/vnd.sumologic.carbon2", req.Header.Get("Content-Type"))
		},
	})
	defer func() { test.srv.Close() }()
	test.exp.config.MaxRequestBodySize = 1

	records := exampleTwoIntMetrics()
	metrics := metricPairToMetrics(records)
	expected := metricPairToMetrics(records[:1])

	dropped, err := test.exp.pushMetricsData(context.Background(), metrics)
	assert.EqualError(t, err, "error during sending data: 500 Internal Server Error")
	assert.Equal(t, 1, dropped)

	partial, ok := err.(consumererror.PartialError)
	require.True(t, ok)
	assert.Equal(t, expected, partial.GetMetrics())
}
