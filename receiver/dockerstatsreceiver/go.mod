module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dockerstatsreceiver

go 1.14

require (
	github.com/census-instrumentation/opencensus-proto v0.3.0
	github.com/docker/docker v20.10.16+incompatible
	github.com/gobwas/glob v0.2.3
	github.com/golang/protobuf v1.4.3
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.0.0-00010101000000-000000000000
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/redisreceiver v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.6.1
	go.opentelemetry.io/collector v0.17.1-0.20210106191745-ca6f1a0287d0
	go.uber.org/zap v1.16.0
	gotest.tools/v3 v3.2.0 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common

replace github.com/open-telemetry/opentelemetry-collector-contrib/receiver/redisreceiver => ../redisreceiver
