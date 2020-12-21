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
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
)

type dataPointCommon interface {
	Timestamp() pdata.TimestampUnixNano
	LabelsMap() pdata.StringMap
}

type prometheusFormatter struct {
	sanitNameRegex *regexp.Regexp
}

type prometheusTags string

func newPrometheusFormatter() (prometheusFormatter, error) {
	sanitNameRegex, err := regexp.Compile(`[^0-9a-zA-Z]`)
	if err != nil {
		return prometheusFormatter{}, nil
	}

	return prometheusFormatter{
		sanitNameRegex: sanitNameRegex,
	}, nil
}

// PrometheusLabels returns all attributes as prometheus labels string
func (f *prometheusFormatter) prometheusTagString(attr pdata.AttributeMap, labels pdata.StringMap) prometheusTags {
	mergedAttributes := pdata.NewAttributeMap()
	attr.CopyTo(mergedAttributes)
	labels.ForEach(func(k string, v string) {
		mergedAttributes.UpsertString(k, v)
	})
	length := mergedAttributes.Len()

	if length == 0 {
		return ""
	}

	returnValue := make([]string, 0, length)
	mergedAttributes.ForEach(func(k string, v pdata.AttributeValue) {
		returnValue = append(
			returnValue,
			fmt.Sprintf(
				`%s="%s"`,
				f.sanitizePrometheusKey(k),
				f.sanitizePrometheusValue(tracetranslator.AttributeValueToString(v, false)),
			),
		)
	})

	return prometheusTags(fmt.Sprintf("{%s}", strings.Join(returnValue, ",")))
}

// sanitizePrometheusKey returns sanitized key string
// (all non-alphanumeric chars replaced with `_`)
func (f *prometheusFormatter) sanitizePrometheusKey(s string) string {
	return f.sanitNameRegex.ReplaceAllString(s, "_")
}

// sanitizePrometheusKey returns sanitized value string
// `/` -> `//`
// `"` -> `\"`
// `\n` -> `\n`
func (f *prometheusFormatter) sanitizePrometheusValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return strings.ReplaceAll(s, `\\n`, `\n`)
}

// prometheusDoubleLine builds metric based on the given arguments where value is float64
// this function shouldn't be called directly
func (f *prometheusFormatter) prometheusDoubleLine(name string, attributes prometheusTags, value float64, timestamp pdata.TimestampUnixNano) string {
	return fmt.Sprintf(
		"%s%s %g %d",
		f.sanitizePrometheusKey(name),
		attributes,
		value,
		timestamp/pdata.TimestampUnixNano(time.Millisecond),
	)
}

// prometheusIntLine builds metric based on the given arguments where value is int64
// this function shouldn't be called directly
func (f *prometheusFormatter) prometheusIntLine(name string, attributes prometheusTags, value int64, timestamp pdata.TimestampUnixNano) string {
	return fmt.Sprintf(
		"%s%s %d %d",
		f.sanitizePrometheusKey(name),
		attributes,
		value,
		timestamp/pdata.TimestampUnixNano(time.Millisecond),
	)
}

// prometheusUIntLine builds metric based on the given arguments where value is uint64
// this function shouldn't be called directly
func (f *prometheusFormatter) prometheusUIntLine(name string, attributes prometheusTags, value uint64, timestamp pdata.TimestampUnixNano) string {
	return fmt.Sprintf(
		"%s%s %d %d",
		f.sanitizePrometheusKey(name),
		attributes,
		value,
		timestamp/pdata.TimestampUnixNano(time.Millisecond),
	)
}

func (f *prometheusFormatter) prometheusDoubleOverwriteValue(name string, value float64, dp dataPointCommon, attributes pdata.AttributeMap) string {
	return f.prometheusDoubleLine(
		name,
		f.prometheusTagString(attributes, dp.LabelsMap()),
		value,
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusIntOverwriteValue(name string, value int64, dp dataPointCommon, attributes pdata.AttributeMap) string {
	return f.prometheusIntLine(
		name,
		f.prometheusTagString(attributes, dp.LabelsMap()),
		value,
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusUIntOverwriteValue(name string, value uint64, dp dataPointCommon, attributes pdata.AttributeMap) string {
	return f.prometheusUIntLine(
		name,
		f.prometheusTagString(attributes, dp.LabelsMap()),
		value,
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusFromDoubleDataPoint(name string, dp pdata.DoubleDataPoint, attributes pdata.AttributeMap) string {
	return f.prometheusDoubleOverwriteValue(
		name,
		dp.Value(),
		dp,
		attributes,
	)
}

func (f *prometheusFormatter) prometheusIntValue(name string, dp pdata.IntDataPoint, attributes pdata.AttributeMap) string {
	return f.prometheusIntLine(
		name,
		f.prometheusTagString(attributes, dp.LabelsMap()),
		dp.Value(),
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusSumMetric(name string) string {
	return fmt.Sprintf("%s_sum", name)
}

func (f *prometheusFormatter) prometheusCountMetric(name string) string {
	return fmt.Sprintf("%s_count", name)
}

func (f *prometheusFormatter) intGauge2Strings(record metricPair) []string {
	dps := record.metric.IntGauge().DataPoints()
	lines := make([]string, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := record.metric.IntGauge().DataPoints().At(i)
		line := f.prometheusIntValue(
			record.metric.Name(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}
	return lines
}

// mergeAttributes gets two pdata.AttributeMaps and returns new which contains values from both of them
func (f *prometheusFormatter) mergeAttributes(attributes pdata.AttributeMap, additionalAttributes pdata.AttributeMap) pdata.AttributeMap {
	mergedAttributes := pdata.NewAttributeMap()
	attributes.CopyTo(mergedAttributes)
	additionalAttributes.ForEach(func(k string, v pdata.AttributeValue) {
		mergedAttributes.Upsert(k, v)
	})
	return mergedAttributes
}

func (f *prometheusFormatter) doubleGauge2Strings(record metricPair) []string {
	dps := record.metric.DoubleGauge().DataPoints()
	lines := make([]string, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		line := f.prometheusFromDoubleDataPoint(
			record.metric.Name(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}

	return lines
}

func (f *prometheusFormatter) intSum2Strings(record metricPair) []string {
	dps := record.metric.IntSum().DataPoints()
	lines := make([]string, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		line := f.prometheusIntValue(
			record.metric.Name(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}

	return lines
}

func (f *prometheusFormatter) doubleSum2Strings(record metricPair) []string {
	dps := record.metric.DoubleSum().DataPoints()
	lines := make([]string, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		line := f.prometheusFromDoubleDataPoint(
			record.metric.Name(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}

	return lines
}

func (f *prometheusFormatter) doubleSummary2Strings(record metricPair) []string {
	dps := record.metric.DoubleSummary().DataPoints()
	var lines []string

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		qs := dp.QuantileValues()
		additionalAttributes := pdata.NewAttributeMap()
		for i := 0; i < qs.Len(); i++ {
			q := qs.At(i)
			additionalAttributes.UpsertDouble("quantile", q.Quantile())

			line := f.prometheusDoubleOverwriteValue(
				record.metric.Name(),
				q.Value(),
				dp,
				f.mergeAttributes(record.attributes, additionalAttributes),
			)
			lines = append(lines, line)
		}

		line := f.prometheusDoubleOverwriteValue(
			f.prometheusSumMetric(record.metric.Name()),
			dp.Sum(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)

		line = f.prometheusUIntOverwriteValue(
			f.prometheusCountMetric(record.metric.Name()),
			dp.Count(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}
	return lines
}

func (f *prometheusFormatter) intHistogram2Strings(record metricPair) []string {
	dps := record.metric.IntHistogram().DataPoints()
	var lines []string

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)

		explicitBounds := dp.ExplicitBounds()
		var cumulative uint64
		additionalAttributes := pdata.NewAttributeMap()

		for i, bound := range explicitBounds {
			cumulative += dp.BucketCounts()[i]
			additionalAttributes.UpsertDouble("le", bound)

			line := f.prometheusUIntOverwriteValue(
				record.metric.Name(),
				cumulative,
				dp,
				f.mergeAttributes(record.attributes, additionalAttributes),
			)
			lines = append(lines, line)
		}

		cumulative += dp.BucketCounts()[len(explicitBounds)]
		additionalAttributes.UpsertString("le", "+Inf")
		line := f.prometheusUIntOverwriteValue(
			record.metric.Name(),
			cumulative,
			dp,
			f.mergeAttributes(record.attributes, additionalAttributes),
		)
		lines = append(lines, line)

		line = f.prometheusIntOverwriteValue(
			f.prometheusSumMetric(record.metric.Name()),
			dp.Sum(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)

		line = f.prometheusUIntOverwriteValue(
			f.prometheusCountMetric(record.metric.Name()),
			dp.Count(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}

	return lines
}

func (f *prometheusFormatter) doubleHistogram2Strings(record metricPair) []string {
	dps := record.metric.DoubleHistogram().DataPoints()
	var lines []string

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)

		explicitBounds := dp.ExplicitBounds()
		var cumulative uint64
		additionalAttributes := pdata.NewAttributeMap()

		for i, bound := range explicitBounds {
			cumulative += dp.BucketCounts()[i]
			additionalAttributes.UpsertDouble("le", bound)

			line := f.prometheusUIntOverwriteValue(
				record.metric.Name(),
				cumulative,
				dp,
				f.mergeAttributes(record.attributes, additionalAttributes),
			)
			lines = append(lines, line)
		}

		cumulative += dp.BucketCounts()[len(explicitBounds)]
		additionalAttributes.UpsertString("le", "+Inf")
		line := f.prometheusUIntOverwriteValue(
			record.metric.Name(),
			cumulative,
			dp,
			f.mergeAttributes(record.attributes, additionalAttributes),
		)
		lines = append(lines, line)

		line = f.prometheusDoubleOverwriteValue(
			f.prometheusSumMetric(record.metric.Name()),
			dp.Sum(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)

		line = f.prometheusUIntOverwriteValue(
			f.prometheusCountMetric(record.metric.Name()),
			dp.Count(),
			dp,
			record.attributes,
		)
		lines = append(lines, line)
	}

	return lines
}

func (f *prometheusFormatter) metric2Prometheus(record metricPair) (string, error) {
	var nextLines []string

	switch record.metric.DataType() {
	case pdata.MetricDataTypeIntGauge:
		nextLines = f.intGauge2Strings(record)
	case pdata.MetricDataTypeDoubleGauge:
		nextLines = f.doubleGauge2Strings(record)
	case pdata.MetricDataTypeIntSum:
		nextLines = f.intSum2Strings(record)
	case pdata.MetricDataTypeDoubleSum:
		nextLines = f.doubleSum2Strings(record)
	case pdata.MetricDataTypeDoubleSummary:
		nextLines = f.doubleSummary2Strings(record)
	case pdata.MetricDataTypeIntHistogram:
		nextLines = f.intHistogram2Strings(record)
	case pdata.MetricDataTypeDoubleHistogram:
		nextLines = f.doubleHistogram2Strings(record)
	}
	return strings.Join(nextLines, "\n"), nil
}
