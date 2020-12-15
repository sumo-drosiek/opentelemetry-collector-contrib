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

	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
)

type dataPointCountable interface {
	dataPointCommon
	Count() int
}

type dataPointValuableDouble interface {
	dataPointCommon
	Value() float64
}

type dataPointValuableInt interface {
	dataPointCommon
	Value() int64
}

type dataPointCommon interface {
	Timestamp() pdata.TimestampUnixNano
	LabelsMap() pdata.StringMap
}

type prometheusFormatter struct {
	sanitNameRegex *regexp.Regexp
}

func newPrometheusFormatter() (prometheusFormatter, error) {
	sanitNameRegex, err := regexp.Compile(`[^0-9a-zA-Z]`)
	if err != nil {
		return prometheusFormatter{}, nil
	}

	return prometheusFormatter{
		sanitNameRegex: sanitNameRegex,
	}, nil
}

func (f *prometheusFormatter) prometheusTagString(attr pdata.AttributeMap, labels pdata.StringMap) string {
	return f.prometheusTagStringWithMerge(attr, pdata.NewAttributeMap(), labels)
}

// PrometheusLabels returns all attributes as prometheus labels string
func (f *prometheusFormatter) prometheusTagStringWithMerge(attr pdata.AttributeMap, additionalAttributes pdata.AttributeMap, labels pdata.StringMap) string {
	mergedAttributes := pdata.NewAttributeMap()
	attr.CopyTo(mergedAttributes)
	additionalAttributes.ForEach(func(k string, v pdata.AttributeValue) {
		mergedAttributes.Upsert(k, v)
	})
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

	return fmt.Sprintf("{%s}", strings.Join(returnValue, ","))
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

// prometheusIntLine builds metric based on the given arguments where value is float64
// this function shouldn't be called directly
func (f *prometheusFormatter) prometheusDoubleLine(name string, attributes string, value float64, timestamp pdata.TimestampUnixNano) string {
	return fmt.Sprintf(
		"%s%s %g %d",
		f.sanitizePrometheusKey(name),
		attributes,
		value,
		timestamp/1e6,
	)
}

// prometheusIntLine builds metric based on the given arguments where value is int64
// this function shouldn't be called directly
func (f *prometheusFormatter) prometheusIntLine(name string, attributes string, value int64, timestamp pdata.TimestampUnixNano) string {
	return fmt.Sprintf(
		"%s%s %d %d",
		f.sanitizePrometheusKey(name),
		attributes,
		value,
		timestamp/1e6,
	)
}

// prometheusIntLine builds metric based on the given arguments where value is uint64
// this function shouldn't be called directly
func (f *prometheusFormatter) prometheusUIntLine(name string, attributes string, value uint64, timestamp pdata.TimestampUnixNano) string {
	return fmt.Sprintf(
		"%s%s %d %d",
		f.sanitizePrometheusKey(name),
		attributes,
		value,
		timestamp/1e6,
	)
}

func (f *prometheusFormatter) prometheusDoubleOverwriteValue(name string, value float64, dp dataPointCommon, attributes pdata.AttributeMap, additionalAttributes pdata.AttributeMap) string {
	return f.prometheusDoubleLine(
		name,
		f.prometheusTagStringWithMerge(attributes, additionalAttributes, dp.LabelsMap()),
		value,
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusIntOverwriteValue(name string, value int64, dp dataPointCommon, attributes pdata.AttributeMap, additionalAttributes pdata.AttributeMap) string {
	return f.prometheusIntLine(
		name,
		f.prometheusTagStringWithMerge(attributes, additionalAttributes, dp.LabelsMap()),
		value,
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusUIntOverwriteValue(name string, value uint64, dp dataPointCommon, attributes pdata.AttributeMap, additionalAttributes pdata.AttributeMap) string {
	return f.prometheusUIntLine(
		name,
		f.prometheusTagStringWithMerge(attributes, additionalAttributes, dp.LabelsMap()),
		value,
		dp.Timestamp(),
	)
}

func (f *prometheusFormatter) prometheusFromDoubleDataPoint(name string, dp dataPointValuableDouble, attributes pdata.AttributeMap, additionalAttributes pdata.AttributeMap) string {
	return f.prometheusDoubleOverwriteValue(
		name,
		dp.Value(),
		dp,
		attributes,
		additionalAttributes,
	)
}

func (f *prometheusFormatter) prometheusIntValue(name string, dp dataPointValuableInt, attributes pdata.AttributeMap, additionalAttributes pdata.AttributeMap) string {
	return f.prometheusIntLine(
		name,
		f.prometheusTagStringWithMerge(attributes, additionalAttributes, dp.LabelsMap()),
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

func (f *prometheusFormatter) metric2Prometheus(record metricPair) (string, error) {
	var nextLines []string
	noAttributes := pdata.NewAttributeMap()

	switch record.metric.DataType() {
	case pdata.MetricDataTypeIntGauge:
		dps := record.metric.IntGauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := record.metric.IntGauge().DataPoints().At(i)
			line := f.prometheusIntValue(
				record.metric.Name(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	case pdata.MetricDataTypeDoubleGauge:
		dps := record.metric.DoubleGauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			line := f.prometheusFromDoubleDataPoint(
				record.metric.Name(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	case pdata.MetricDataTypeIntSum:
		dps := record.metric.IntSum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			line := f.prometheusIntValue(
				record.metric.Name(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	case pdata.MetricDataTypeDoubleSum:
		dps := record.metric.DoubleSum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			line := f.prometheusFromDoubleDataPoint(
				record.metric.Name(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	case pdata.MetricDataTypeDoubleSummary:
		dps := record.metric.DoubleSummary().DataPoints()
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
					record.attributes,
					additionalAttributes,
				)
				nextLines = append(nextLines, line)
			}

			line := f.prometheusDoubleOverwriteValue(
				f.prometheusSumMetric(record.metric.Name()),
				dp.Sum(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)

			line = f.prometheusUIntOverwriteValue(
				f.prometheusCountMetric(record.metric.Name()),
				dp.Count(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	case pdata.MetricDataTypeIntHistogram:
		dps := record.metric.IntHistogram().DataPoints()
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
					record.attributes,
					additionalAttributes,
				)
				nextLines = append(nextLines, line)
			}

			cumulative += dp.BucketCounts()[len(explicitBounds)]
			additionalAttributes.UpsertString("le", "+Inf")
			line := f.prometheusUIntOverwriteValue(
				record.metric.Name(),
				cumulative,
				dp,
				record.attributes,
				additionalAttributes,
			)
			nextLines = append(nextLines, line)

			line = f.prometheusIntOverwriteValue(
				f.prometheusSumMetric(record.metric.Name()),
				dp.Sum(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)

			line = f.prometheusUIntOverwriteValue(
				f.prometheusCountMetric(record.metric.Name()),
				dp.Count(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	case pdata.MetricDataTypeDoubleHistogram:
		dps := record.metric.DoubleHistogram().DataPoints()
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
					record.attributes,
					additionalAttributes,
				)
				nextLines = append(nextLines, line)
			}

			cumulative += dp.BucketCounts()[len(explicitBounds)]
			additionalAttributes.UpsertString("le", "+Inf")
			line := f.prometheusUIntOverwriteValue(
				record.metric.Name(),
				cumulative,
				dp,
				record.attributes,
				additionalAttributes,
			)
			nextLines = append(nextLines, line)

			line = f.prometheusDoubleOverwriteValue(
				f.prometheusSumMetric(record.metric.Name()),
				dp.Sum(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)

			line = f.prometheusUIntOverwriteValue(
				f.prometheusCountMetric(record.metric.Name()),
				dp.Count(),
				dp,
				record.attributes,
				noAttributes,
			)
			nextLines = append(nextLines, line)
		}
	}
	return strings.Join(nextLines, "\n"), nil
}
