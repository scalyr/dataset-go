module readme

go 1.19

require (
	github.com/scalyr/dataset-go v0.0.0
	go.opentelemetry.io/otel v1.20.0
	go.uber.org/zap v1.26.0
)

require (
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cskr/pubsub v1.0.2 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.4.0 // indirect
	go.opentelemetry.io/otel/metric v1.20.0 // indirect
	go.opentelemetry.io/otel/trace v1.20.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
)

replace github.com/scalyr/dataset-go v0.0.0 => ./../..
