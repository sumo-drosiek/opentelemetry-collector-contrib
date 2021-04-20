// Copyright 2021 OpenTelemetry Authors
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

package tcpreceiver

import (
	"context"
	"fmt"
	"net"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/stanza"
)

func TestSyslogWithTcp(t *testing.T) {
	testTCP(t, testdataConfigYamlAsMap())
}

func testTCP(t *testing.T, cfg *TCPConfig) {
	numLogs := 5

	f := NewFactory()
	params := component.ReceiverCreateParams{Logger: zaptest.NewLogger(t)}
	sink := new(consumertest.LogsSink)
	rcvr, err := f.CreateLogsReceiver(context.Background(), params, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcvr.Start(context.Background(), &testHost{t: t}))

	var conn net.Conn
	conn, err = net.Dial("tcp", "0.0.0.0:29018")
	require.NoError(t, err)

	for i := 0; i < numLogs; i++ {
		msg := fmt.Sprintf("<86>1 2021-02-28T00:0%d:02.003Z 192.168.1.1 SecureAuth0 23108 ID52020 [SecureAuth@27389] test msg %d\n", i, i)
		_, err = conn.Write([]byte(msg))
		require.NoError(t, err)
	}
	require.NoError(t, conn.Close())

	require.Eventually(t, expectNLogs(sink, numLogs), 2*time.Second, time.Millisecond)
	require.NoError(t, rcvr.Shutdown(context.Background()))
	require.Len(t, sink.AllLogs(), 1)

	resourceLogs := sink.AllLogs()[0].ResourceLogs().At(0)
	logs := resourceLogs.InstrumentationLibraryLogs().At(0).Logs()

	for i := 0; i < numLogs; i++ {
		log := logs.At(i)

		msg := log.Body()
		require.Equal(t, msg.StringVal(), fmt.Sprintf("<86>1 2021-02-28T00:0%d:02.003Z 192.168.1.1 SecureAuth0 23108 ID52020 [SecureAuth@27389] test msg %d", i, i))
	}
}

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.Nil(t, err)

	factory := NewFactory()
	factories.Receivers[config.Type(typeStr)] = factory
	cfg, err := configtest.LoadConfigFile(
		t, path.Join(".", "testdata", "config.yaml"), factories,
	)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Receivers), 1)
	assert.Equal(t, testdataConfigYamlAsMap(), cfg.Receivers["tcp"])
}

func testdataConfigYamlAsMap() *TCPConfig {
	return &TCPConfig{
		BaseConfig: stanza.BaseConfig{
			ReceiverSettings: config.ReceiverSettings{
				TypeVal: "tcp",
				NameVal: "tcp",
			},
			Operators: stanza.OperatorConfigs{},
		},
		Input: stanza.InputConfig{
			"listen_address": "0.0.0.0:29018",
		},
	}
}

func TestDecodeInputConfigFailure(t *testing.T) {
	params := component.ReceiverCreateParams{
		Logger: zap.NewNop(),
	}
	sink := new(consumertest.LogsSink)
	factory := NewFactory()
	badCfg := &TCPConfig{
		BaseConfig: stanza.BaseConfig{
			ReceiverSettings: config.ReceiverSettings{
				TypeVal: "tcp",
				NameVal: "tcp",
			},
			Operators: stanza.OperatorConfigs{},
		},
		Input: stanza.InputConfig{
			"max_buffer_size": "0.1.0.1-",
		},
	}
	receiver, err := factory.CreateLogsReceiver(context.Background(), params, badCfg, sink)
	require.Error(t, err, "receiver creation should fail if input config isn't valid")
	require.Nil(t, receiver, "receiver creation should fail if input config isn't valid")
}

func expectNLogs(sink *consumertest.LogsSink, expected int) func() bool {
	return func() bool {
		return sink.LogRecordsCount() == expected
	}
}

type testHost struct {
	component.Host
	t *testing.T
}

var _ component.Host = (*testHost)(nil)

// ReportFatalError causes the test to be run to fail.
func (h *testHost) ReportFatalError(err error) {
	h.t.Fatalf("receiver reported a fatal error: %v", err)
}
