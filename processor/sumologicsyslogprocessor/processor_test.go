// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sumologicsyslogprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/pdata"
)

func TestProcessLogs(t *testing.T) {
	lines := []string{
		`<13> Example log`,
		`<334> Another example log`,
		`Plain text`,
	}

	facilities := []string{
		`user-level messages`,
		`syslog`,
		`syslog`,
	}

	buffer := make([]pdata.LogRecord, 3)
	buffer[0] = pdata.NewLogRecord()
	buffer[0].Body().SetStringVal("<13> Example log")
	buffer[1] = pdata.NewLogRecord()
	buffer[1].Body().SetStringVal("<334> Another example log")
	buffer[2] = pdata.NewLogRecord()
	buffer[2].Body().SetStringVal("Plain text")

	logs := pdata.NewLogs()
	logs.ResourceLogs().Resize(1)
	logs.ResourceLogs().At(0).InstrumentationLibraryLogs().Resize(len(lines))
	for _, line := range lines {
		lr := pdata.NewLogRecord()
		lr.Body().SetStringVal(line)
		logs.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).Logs().Append(lr)
	}

	ctx := context.Background()
	processor := &sumologicSyslogProcessor{
		syslogFacilityAttrName: "facility_name",
	}

	result, err := processor.ProcessLogs(ctx, logs)
	require.NoError(t, err)

	for i, line := range facilities {
		attrs := result.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).Logs().At(i).Attributes()
		attr, ok := attrs.Get("facility_name")
		require.True(t, ok)
		assert.Equal(t, line, attr.StringVal())
	}
}
