// Copyright The OpenTelemetry Authors
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

package datasenders

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/testbed/testbed"
)

// FileLogK8sWriter represents abstract container k8s writer
type FileLogK8sWriter struct {
	testbed.DataSenderBase
	file   *os.File
	config string
}

// Ensure FileLogK8sWriter implements LogDataSender.
var _ testbed.LogDataSender = (*FileLogK8sWriter)(nil)

// NewFileLogK8sWriter creates a new data sender that will write kubernetes containerd
// log entries to a file, to be tailed by FileLogReceiver and sent to the collector.
//
// config is an OTC config appended to the receivers section after executing fmt.Sprintf on it.
// This impicated few things:
//   - it should contain `%s` which will be replaced with the filename
//   - all `%` should be represented as `%%`
//   - indentation style matters. Spaces have to be used for indentation
//     and it should start with two spaces indentation
//
// Example config:
// |`
// |  filelog:
// |    include: [ %s ]
// |    start_at: beginning
// |    operators:
// |      type: regex_parser
// |      regex: ^(?P<log>.*)$
// |  `
func NewFileLogK8sWriter(config string) *FileLogK8sWriter {
	dir, err := ioutil.TempDir("", "namespace-*_test-pod_000011112222333344445555666677778888")
	if err != nil {
		panic("failed to create temp dir")
	}
	dir, err = ioutil.TempDir(dir, "*")
	if err != nil {
		panic("failed to create temp dir")
	}

	file, err := ioutil.TempFile(dir, "*.log")
	if err != nil {
		panic("failed to create temp file")
	}

	f := &FileLogK8sWriter{
		file:   file,
		config: config,
	}

	return f
}

func (f *FileLogK8sWriter) Start() error {
	return nil
}

func (f *FileLogK8sWriter) ConsumeLogs(_ context.Context, logs pdata.Logs) error {
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		for j := 0; j < logs.ResourceLogs().At(i).InstrumentationLibraryLogs().Len(); j++ {
			ills := logs.ResourceLogs().At(i).InstrumentationLibraryLogs().At(j)
			for k := 0; k < ills.Logs().Len(); k++ {
				_, err := f.file.Write(append(f.convertLogToTextLine(ills.Logs().At(k)), '\n'))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (f *FileLogK8sWriter) convertLogToTextLine(lr pdata.LogRecord) []byte {
	sb := strings.Builder{}

	// Timestamp
	sb.WriteString(time.Unix(0, int64(lr.Timestamp())).Format("2006-01-02T15:04:05.000000000Z"))

	// Severity
	sb.WriteString(" stderr F ")
	sb.WriteString(lr.SeverityText())
	sb.WriteString(" ")

	if lr.Body().Type() == pdata.AttributeValueSTRING {
		sb.WriteString(lr.Body().StringVal())
	}

	lr.Attributes().ForEach(func(k string, v pdata.AttributeValue) {
		sb.WriteString(" ")
		sb.WriteString(k)
		sb.WriteString("=")
		switch v.Type() {
		case pdata.AttributeValueSTRING:
			sb.WriteString(v.StringVal())
		case pdata.AttributeValueINT:
			sb.WriteString(strconv.FormatInt(v.IntVal(), 10))
		case pdata.AttributeValueDOUBLE:
			sb.WriteString(strconv.FormatFloat(v.DoubleVal(), 'f', -1, 64))
		case pdata.AttributeValueBOOL:
			sb.WriteString(strconv.FormatBool(v.BoolVal()))
		default:
			panic("missing case")
		}
	})

	return []byte(sb.String())
}

func (f *FileLogK8sWriter) Flush() {
	_ = f.file.Sync()
}

func (f *FileLogK8sWriter) GenConfigYAMLStr() string {
	// Note that this generates a receiver config for agent.
	// We are testing filelog receiver here.

	return fmt.Sprintf(f.config, f.file.Name())
}

func (f *FileLogK8sWriter) ProtocolName() string {
	return "filelog"
}

func (f *FileLogK8sWriter) GetEndpoint() string {
	return ""
}

// NewFileLogContainerdWriter returns FileLogK8sWriter with configuration,
// to recognize and parse kubernetes containerd logs
func NewFileLogContainerdWriter() *FileLogK8sWriter {
	return NewFileLogK8sWriter(`
  filelog:
    include: [ %s ]
    start_at: beginning
    include_file_path: true
    include_file_name: false
    operators:
      # Find out which format is used by kubernetes
      - type: router
        id: get-format
        routes:
          - output: parser-docker
            expr: '$$record matches "^\\{"'
          - output: parser-crio
            expr: '$$record matches "^[^ Z]+ "'
          - output: parser-containerd
            expr: '$$record matches "^[^ Z]+Z"'
      # Parse CRI-O format
      - type: regex_parser
        id: parser-crio
        regex: '^(?P<time>[^ Z]+) (?P<stream>stdout|stderr) (?P<logtag>[^ ]*) (?P<log>.*)$'
        output: extract_metadata_from_filepath
        timestamp:
          parse_from: time
          layout_type: gotime
          layout: '2006-01-02T15:04:05.000000000-07:00'
      # Parse CRI-Containerd format
      - type: regex_parser
        id: parser-containerd
        regex: '^(?P<time>[^ ^Z]+Z) (?P<stream>stdout|stderr) (?P<logtag>[^ ]*) (?P<log>.*)$'
        output: extract_metadata_from_filepath
        timestamp:
          parse_from: time
          layout: '%%Y-%%m-%%dT%%H:%%M:%%S.%%LZ'
      # Parse Docker format
      - type: json_parser
        id: parser-docker
        output: extract_metadata_from_filepath
        timestamp:
          parse_from: time
          layout: '%%Y-%%m-%%dT%%H:%%M:%%S.%%LZ'
      # Extract metadata from file path
      - type: regex_parser
        id: extract_metadata_from_filepath
        regex: '^.*\/(?P<namespace>[^_]+)_(?P<pod_name>[^_]+)_(?P<uid>[a-f0-9\-]{36})\/(?P<container_name>[^\._]+)\/(?P<run_id>\d+)\.log$'
        parse_from: $$labels.file_path
      # Move out attributes to Attributes
      - type: metadata
        labels:
          stream: 'EXPR($.stream)'
          k8s.container.name: 'EXPR($.container_name)'
          k8s.namespace.name: 'EXPR($.namespace)'
          k8s.pod.name: 'EXPR($.pod_name)'
          run_id: 'EXPR($.run_id)'
          k8s.pod.uid: 'EXPR($.uid)'
      # Clean up log record
      - type: restructure
        id: clean-up-log-record
        ops:
          - remove: logtag
          - remove: stream
          - remove: container_name
          - remove: namespace
          - remove: pod_name
          - remove: run_id
          - remove: uid
  `)
}
