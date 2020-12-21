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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/pdata"
)

func newTestPrometheusFormatter(t *testing.T) prometheusFormatter {
	pf, err := newPrometheusFormatter()
	require.NoError(t, err)

	return pf
}

func TestsanitizeKey(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	key := "&^*123-abc-ABC!?"
	expected := "___123_abc_ABC__"
	assert.Equal(t, expected, f.sanitizeKey(key))
}

func TestsanitizeValue(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	value := `&^*123-abc-ABC!?"\\n`
	expected := `&^*123-abc-ABC!?\"\\\n`
	assert.Equal(t, expected, f.sanitizeValue(value))
}

func Testtags2StringNoAttributes(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	mp := exampleTwoIntMetrics()[0]
	mp.attributes.InitEmptyWithCapacity(0)
	assert.Equal(t, prometheusTags(""), f.tags2String(mp.attributes, pdata.NewStringMap()))
}

func Testtags2String(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	mp := exampleTwoIntMetrics()[0]
	assert.Equal(
		t,
		prometheusTags(`{test="test_value",test2="second_value"}`),
		f.tags2String(mp.attributes, pdata.NewStringMap()),
	)
}

func TestPrometheusMetricDataTypeIntGauge(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeIntGauge)
	metric.metric.IntGauge().InitEmpty()
	metric.metric.SetName("gauge_metric_name")

	metric.attributes.InsertString("foo", "bar")

	dp := pdata.NewIntDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("remote_name", "156920")
	dp.LabelsMap().Insert("url", "http://example_url")
	dp.SetValue(124)
	dp.SetTimestamp(1608124661.166 * 1e9)
	metric.metric.IntGauge().DataPoints().Append(dp)

	dp = pdata.NewIntDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("remote_name", "156955")
	dp.LabelsMap().Insert("url", "http://another_url")
	dp.SetValue(245)
	dp.SetTimestamp(1608124662.166 * 1e9)
	metric.metric.IntGauge().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `gauge_metric_name{foo="bar",remote_name="156920",url="http://example_url"} 124 1608124661166
gauge_metric_name{foo="bar",remote_name="156955",url="http://another_url"} 245 1608124662166`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestPrometheusMetricDataTypeDoubleGauge(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeDoubleGauge)
	metric.metric.DoubleGauge().InitEmpty()
	metric.metric.SetName("gauge_metric_name_double_test")

	metric.attributes.InsertString("foo", "bar")

	dp := pdata.NewDoubleDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("local_name", "156720")
	dp.LabelsMap().Insert("endpoint", "http://example_url")
	dp.SetValue(33.4)
	dp.SetTimestamp(1608124661.169 * 1e9)
	metric.metric.DoubleGauge().DataPoints().Append(dp)

	dp = pdata.NewDoubleDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("local_name", "156155")
	dp.LabelsMap().Insert("endpoint", "http://another_url")
	dp.SetValue(56.8)
	dp.SetTimestamp(1608124662.186 * 1e9)
	metric.metric.DoubleGauge().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `gauge_metric_name_double_test{foo="bar",local_name="156720",endpoint="http://example_url"} 33.4 1608124661169
gauge_metric_name_double_test{foo="bar",local_name="156155",endpoint="http://another_url"} 56.8 1608124662186`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestPrometheusMetricDataTypeIntSum(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeIntSum)
	metric.metric.IntSum().InitEmpty()
	metric.metric.SetName("sum_metric_int_test")

	metric.attributes.InsertString("foo", "bar")

	dp := pdata.NewIntDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("name", "156720")
	dp.LabelsMap().Insert("address", "http://example_url")
	dp.SetValue(45)
	dp.SetTimestamp(1608124444.169 * 1e9)
	metric.metric.IntSum().DataPoints().Append(dp)

	dp = pdata.NewIntDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("name", "156155")
	dp.LabelsMap().Insert("address", "http://another_url")
	dp.SetValue(1238)
	dp.SetTimestamp(1608124699.186 * 1e9)
	metric.metric.IntSum().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `sum_metric_int_test{foo="bar",name="156720",address="http://example_url"} 45 1608124444169
sum_metric_int_test{foo="bar",name="156155",address="http://another_url"} 1238 1608124699186`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestPrometheusMetricDataTypeDoubleSum(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeDoubleSum)
	metric.metric.DoubleSum().InitEmpty()
	metric.metric.SetName("sum_metric_double_test")

	metric.attributes.InsertString("foo", "bar")

	dp := pdata.NewDoubleDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("pod_name", "lorem")
	dp.LabelsMap().Insert("namespace", "default")
	dp.SetValue(45.6)
	dp.SetTimestamp(1618124444.169 * 1e9)
	metric.metric.DoubleSum().DataPoints().Append(dp)

	dp = pdata.NewDoubleDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("pod_name", "opsum")
	dp.LabelsMap().Insert("namespace", "kube-config")
	dp.SetValue(1238.1)
	dp.SetTimestamp(1608424699.186 * 1e9)
	metric.metric.DoubleSum().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `sum_metric_double_test{foo="bar",pod_name="lorem",namespace="default"} 45.6 1618124444169
sum_metric_double_test{foo="bar",pod_name="opsum",namespace="kube-config"} 1238.1 1608424699186`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestPrometheusMetricDataTypeDoubleSummary(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeDoubleSummary)
	metric.metric.DoubleSummary().InitEmpty()
	metric.metric.SetName("summary_metric_double_test")

	metric.attributes.InsertString("foo", "bar")

	dp := pdata.NewDoubleSummaryDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("pod_name", "dolor")
	dp.LabelsMap().Insert("namespace", "sumologic")
	dp.SetSum(45.6)
	dp.SetCount(3)
	dp.SetTimestamp(1618124444.169 * 1e9)

	quantile := pdata.NewValueAtQuantile()
	quantile.InitEmpty()
	quantile.SetQuantile(0.6)
	quantile.SetValue(0.7)
	dp.QuantileValues().Append(quantile)

	quantile = pdata.NewValueAtQuantile()
	quantile.InitEmpty()
	quantile.SetQuantile(2.6)
	quantile.SetValue(4)
	dp.QuantileValues().Append(quantile)

	metric.metric.DoubleSummary().DataPoints().Append(dp)

	dp = pdata.NewDoubleSummaryDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("pod_name", "sit")
	dp.LabelsMap().Insert("namespace", "main")
	dp.SetSum(1238.1)
	dp.SetCount(7)
	dp.SetTimestamp(1608424699.186 * 1e9)
	metric.metric.DoubleSummary().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `summary_metric_double_test{foo="bar",quantile="0.6",pod_name="dolor",namespace="sumologic"} 0.7 1618124444169
summary_metric_double_test{foo="bar",quantile="2.6",pod_name="dolor",namespace="sumologic"} 4 1618124444169
summary_metric_double_test_sum{foo="bar",pod_name="dolor",namespace="sumologic"} 45.6 1618124444169
summary_metric_double_test_count{foo="bar",pod_name="dolor",namespace="sumologic"} 3 1618124444169
summary_metric_double_test_sum{foo="bar",pod_name="sit",namespace="main"} 1238.1 1608424699186
summary_metric_double_test_count{foo="bar",pod_name="sit",namespace="main"} 7 1608424699186`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestPrometheusMetricDataTypeIntHistogram(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeIntHistogram)
	metric.metric.IntHistogram().InitEmpty()
	metric.metric.SetName("histogram_metric_int_test")

	metric.attributes.InsertString("foo", "bar")

	dp := pdata.NewIntHistogramDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("pod_name", "dolor")
	dp.LabelsMap().Insert("namespace", "sumologic")
	dp.SetBucketCounts([]uint64{0, 12, 7, 5, 8, 13})
	dp.SetExplicitBounds([]float64{0.1, 0.2, 0.5, 0.8, 1})
	dp.SetTimestamp(1618124444.169 * 1e9)
	dp.SetSum(45)
	dp.SetCount(3)
	metric.metric.IntHistogram().DataPoints().Append(dp)

	dp = pdata.NewIntHistogramDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("pod_name", "sit")
	dp.LabelsMap().Insert("namespace", "main")
	dp.SetBucketCounts([]uint64{0, 10, 1, 1, 4, 6})
	dp.SetExplicitBounds([]float64{0.1, 0.2, 0.5, 0.8, 1})
	dp.SetTimestamp(1608424699.186 * 1e9)
	dp.SetSum(54)
	dp.SetCount(5)
	metric.metric.IntHistogram().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `histogram_metric_int_test{foo="bar",le="0.1",pod_name="dolor",namespace="sumologic"} 0 1618124444169
histogram_metric_int_test{foo="bar",le="0.2",pod_name="dolor",namespace="sumologic"} 12 1618124444169
histogram_metric_int_test{foo="bar",le="0.5",pod_name="dolor",namespace="sumologic"} 19 1618124444169
histogram_metric_int_test{foo="bar",le="0.8",pod_name="dolor",namespace="sumologic"} 24 1618124444169
histogram_metric_int_test{foo="bar",le="1",pod_name="dolor",namespace="sumologic"} 32 1618124444169
histogram_metric_int_test{foo="bar",le="+Inf",pod_name="dolor",namespace="sumologic"} 45 1618124444169
histogram_metric_int_test_sum{foo="bar",pod_name="dolor",namespace="sumologic"} 45 1618124444169
histogram_metric_int_test_count{foo="bar",pod_name="dolor",namespace="sumologic"} 3 1618124444169
histogram_metric_int_test{foo="bar",le="0.1",pod_name="sit",namespace="main"} 0 1608424699186
histogram_metric_int_test{foo="bar",le="0.2",pod_name="sit",namespace="main"} 10 1608424699186
histogram_metric_int_test{foo="bar",le="0.5",pod_name="sit",namespace="main"} 11 1608424699186
histogram_metric_int_test{foo="bar",le="0.8",pod_name="sit",namespace="main"} 12 1608424699186
histogram_metric_int_test{foo="bar",le="1",pod_name="sit",namespace="main"} 16 1608424699186
histogram_metric_int_test{foo="bar",le="+Inf",pod_name="sit",namespace="main"} 22 1608424699186
histogram_metric_int_test_sum{foo="bar",pod_name="sit",namespace="main"} 54 1608424699186
histogram_metric_int_test_count{foo="bar",pod_name="sit",namespace="main"} 5 1608424699186`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestPrometheusMetricDataTypeDoubleHistogram(t *testing.T) {
	f := newTestPrometheusFormatter(t)

	metric := metricPair{
		attributes: pdata.NewAttributeMap(),
		metric:     pdata.NewMetric(),
	}

	metric.metric.InitEmpty()
	metric.metric.SetDataType(pdata.MetricDataTypeDoubleHistogram)
	metric.metric.DoubleHistogram().InitEmpty()
	metric.metric.SetName("histogram_metric_double_test")

	metric.attributes.InsertString("bar", "foo")

	dp := pdata.NewDoubleHistogramDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("container", "dolor")
	dp.LabelsMap().Insert("branch", "sumologic")
	dp.SetBucketCounts([]uint64{0, 12, 7, 5, 8, 13})
	dp.SetExplicitBounds([]float64{0.1, 0.2, 0.5, 0.8, 1})
	dp.SetTimestamp(1618124444.169 * 1e9)
	dp.SetSum(45.6)
	dp.SetCount(7)
	metric.metric.DoubleHistogram().DataPoints().Append(dp)

	dp = pdata.NewDoubleHistogramDataPoint()
	dp.InitEmpty()
	dp.LabelsMap().Insert("container", "sit")
	dp.LabelsMap().Insert("branch", "main")
	dp.SetBucketCounts([]uint64{0, 10, 1, 1, 4, 6})
	dp.SetExplicitBounds([]float64{0.1, 0.2, 0.5, 0.8, 1})
	dp.SetTimestamp(1608424699.186 * 1e9)
	dp.SetSum(54.1)
	dp.SetCount(98)
	metric.metric.DoubleHistogram().DataPoints().Append(dp)

	result, err := f.metric2String(metric)
	expected := `histogram_metric_double_test{bar="foo",le="0.1",container="dolor",branch="sumologic"} 0 1618124444169
histogram_metric_double_test{bar="foo",le="0.2",container="dolor",branch="sumologic"} 12 1618124444169
histogram_metric_double_test{bar="foo",le="0.5",container="dolor",branch="sumologic"} 19 1618124444169
histogram_metric_double_test{bar="foo",le="0.8",container="dolor",branch="sumologic"} 24 1618124444169
histogram_metric_double_test{bar="foo",le="1",container="dolor",branch="sumologic"} 32 1618124444169
histogram_metric_double_test{bar="foo",le="+Inf",container="dolor",branch="sumologic"} 45 1618124444169
histogram_metric_double_test_sum{bar="foo",container="dolor",branch="sumologic"} 45.6 1618124444169
histogram_metric_double_test_count{bar="foo",container="dolor",branch="sumologic"} 7 1618124444169
histogram_metric_double_test{bar="foo",le="0.1",container="sit",branch="main"} 0 1608424699186
histogram_metric_double_test{bar="foo",le="0.2",container="sit",branch="main"} 10 1608424699186
histogram_metric_double_test{bar="foo",le="0.5",container="sit",branch="main"} 11 1608424699186
histogram_metric_double_test{bar="foo",le="0.8",container="sit",branch="main"} 12 1608424699186
histogram_metric_double_test{bar="foo",le="1",container="sit",branch="main"} 16 1608424699186
histogram_metric_double_test{bar="foo",le="+Inf",container="sit",branch="main"} 22 1608424699186
histogram_metric_double_test_sum{bar="foo",container="sit",branch="main"} 54.1 1608424699186
histogram_metric_double_test_count{bar="foo",container="sit",branch="main"} 98 1608424699186`

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}
