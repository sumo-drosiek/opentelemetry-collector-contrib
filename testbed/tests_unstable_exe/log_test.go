// Copyright 2019, OpenTelemetry Authors
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

// Package tests contains test cases. To run the tests go to tests directory and run:
// RUN_TESTBED=1 go test -v

package tests

import (
	"testing"

	"go.opentelemetry.io/collector/testbed/testbed"
	scenarios "go.opentelemetry.io/collector/testbed/tests"

	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/datasenders"
)

var contribPerfResultsSummary testbed.TestResultsSummary = &testbed.PerformanceResults{}

// TestMain is used to initiate setup, execution and tear down of testbed.
func TestMain(m *testing.M) {
	// These tests are using the unstable executable.
	testbed.GlobalConfig.DefaultAgentExeRelativeFile = "../../bin/otelcontribcol_unstable_{{.GOOS}}_{{.GOARCH}}"
	testbed.DoTestMain(m, contribPerfResultsSummary)
}

func TestLogMaxDPS(t *testing.T) {
	tests := []struct {
		name         string
		sender       testbed.DataSender
		receiver     testbed.DataReceiver
		resourceSpec testbed.ResourceSpec
		extensions   map[string]string
	}{
		{
			name:     "OTLP",
			sender:   testbed.NewOTLPLogsDataSender(testbed.DefaultHost, testbed.GetAvailablePort(t)),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "filelog",
			sender:   datasenders.NewFileLogWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8d containerd",
			sender:   datasenders.NewFileLogContainerdWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto detection",
			sender:   datasenders.NewFileLogK8sAutoDetectionWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto detection routed",
			sender:   datasenders.NewFileLogK8sAutoDetectionRoutedWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto routed only",
			sender:   datasenders.NewFileLogK8sAutoDetectionRoutedOnlyWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto routed filepath only",
			sender:   datasenders.NewFileLogK8sAutoDetectionRoutedFilePathWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
	}

	processors := map[string]string{
		"batch": `
  batch:
`,
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenarios.ScenarioMaxItemsPerSecond(
				t,
				test.sender,
				test.receiver,
				test.resourceSpec,
				contribPerfResultsSummary,
				processors,
				test.extensions,
			)
		})
	}
}

func TestLog10kDPS(t *testing.T) {
	tests := []struct {
		name         string
		sender       testbed.DataSender
		receiver     testbed.DataReceiver
		resourceSpec testbed.ResourceSpec
		extensions   map[string]string
	}{
		{
			name:     "OTLP",
			sender:   testbed.NewOTLPLogsDataSender(testbed.DefaultHost, testbed.GetAvailablePort(t)),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "filelog",
			sender:   datasenders.NewFileLogWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8d containerd",
			sender:   datasenders.NewFileLogContainerdWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto detection",
			sender:   datasenders.NewFileLogK8sAutoDetectionWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto detection routed",
			sender:   datasenders.NewFileLogK8sAutoDetectionRoutedWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto routed only",
			sender:   datasenders.NewFileLogK8sAutoDetectionRoutedOnlyWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
		{
			name:     "k8s auto routed filepath only",
			sender:   datasenders.NewFileLogK8sAutoDetectionRoutedFilePathWriter(),
			receiver: testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 200,
				ExpectedMaxRAM: 300,
			},
		},
	}

	processors := map[string]string{
		"batch": `
  batch:
`,
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenarios.Scenario10kItemsPerSecond(
				t,
				test.sender,
				test.receiver,
				test.resourceSpec,
				contribPerfResultsSummary,
				processors,
				test.extensions,
			)
		})
	}
}
