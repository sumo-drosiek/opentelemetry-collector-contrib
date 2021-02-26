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

package journaldreceiver

import (
	"github.com/open-telemetry/opentelemetry-log-collection/operator/builtin/input/journald"
	"github.com/open-telemetry/opentelemetry-log-collection/operator"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"gopkg.in/yaml.v2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/stanzareceiver"
)

const typeStr = "journald"

// NewFactory creates a factory for filelog receiver
func NewFactory() component.ReceiverFactory {
	return stanzareceiver.NewFactory(ReceiverType{})
}

// ReceiverType implements stanzareceiver.LogReceiverType
// to create a file tailing receiver
type ReceiverType struct{}

// Type is the receiver type
func (f ReceiverType) Type() configmodels.Type {
	return configmodels.Type(typeStr)
}

// CreateDefaultConfig creates a config with type and version
func (f ReceiverType) CreateDefaultConfig() configmodels.Receiver {
	return createDefaultConfig()
}
func createDefaultConfig() *JournaldConfig {
	return &JournaldConfig{
		BaseConfig: stanzareceiver.BaseConfig{
			ReceiverSettings: configmodels.ReceiverSettings{
				TypeVal: configmodels.Type(typeStr),
				NameVal: typeStr,
			},
			Operators: stanzareceiver.OperatorConfigs{},
		},
		Input: stanzareceiver.InputConfig{},
	}
}

// BaseConfig gets the base config from config, for now
func (f ReceiverType) BaseConfig(cfg configmodels.Receiver) stanzareceiver.BaseConfig {
	return cfg.(*JournaldConfig).BaseConfig
}

// FileLogConfig defines configuration for the filelog receiver
type JournaldConfig struct {
	stanzareceiver.BaseConfig `mapstructure:",squash"`
	Input                     stanzareceiver.InputConfig `mapstructure:",remain"`
}

// DecodeInputConfig unmarshals the input operator
func (f ReceiverType) DecodeInputConfig(cfg configmodels.Receiver) (*operator.Config, error) {
	logConfig := cfg.(*JournaldConfig)
	yamlBytes, _ := yaml.Marshal(logConfig.Input)
	inputCfg := journald.NewJournaldInputConfig("journald_input")
	if err := yaml.Unmarshal(yamlBytes, &inputCfg); err != nil {
		return nil, err
	}
	return &operator.Config{Builder: inputCfg}, nil
}
