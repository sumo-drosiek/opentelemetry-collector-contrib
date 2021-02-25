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

package statsexporter

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type statsexporter struct {
	startAllTime   time.Time
	allTimeCounter int
	startTickTime  time.Time
	tickCounter    int
	tickLength     time.Duration
}

func initExporter(cfg *Config) (*statsexporter, error) {
	tl, err := time.ParseDuration("10s")
	if err != nil {
		return nil, err
	}

	se := &statsexporter{
		startAllTime:   time.Now(),
		allTimeCounter: 0,
		startTickTime:  time.Now(),
		tickCounter:    0,
		tickLength:     tl,
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
		exporterhelper.WithShutdown(se.stop),
	)
}

func (se *statsexporter) stop(ctx context.Context) (err error) {
	diff := time.Now().Sub(se.startAllTime)
	fmt.Printf("Processed in total %d logs in %ds: %f logs/sec\n", se.allTimeCounter, int(diff.Seconds()), float64(se.allTimeCounter)/diff.Seconds())
	return nil
}

// pushLogsData groups data with common metadata and sends them as separate batched requests.
// It returns the number of unsent logs and an error which contains a list of dropped records
// so they can be handled by OTC retry mechanism
func (se *statsexporter) pushLogsData(ctx context.Context, ld pdata.Logs) (int, error) {
	if se.allTimeCounter == 0 {
		now := time.Now()
		se.startAllTime = now
		se.startTickTime = now
	}
	// Iterate over ResourceLogs
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)

		ills := rl.InstrumentationLibraryLogs()
		// iterate over InstrumentationLibraryLogs
		for j := 0; j < ills.Len(); j++ {
			ill := ills.At(j)

			se.allTimeCounter += ill.Logs().Len()
			se.tickCounter += ill.Logs().Len()

			now := time.Now()
			diff := now.Sub(se.startTickTime)

			if diff > se.tickLength {
				fmt.Printf("Processed %d logs in %ds: %f logs/sec\n", se.tickCounter, int(diff.Seconds()), float64(se.tickCounter)/diff.Seconds())
				se.tickCounter = 0
				se.startTickTime = now
			}
		}
	}

	return 0, nil
}
