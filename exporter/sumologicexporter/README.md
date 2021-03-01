# Sumo Logic Exporter

This exporter supports sending logs and metrics data to [Sumo Logic](https://www.sumologic.com/).

The following configuration options are supported:

- `endpoint` (required): Unique URL generated for your HTTP Metrics Source. This is the address to send metrics to.
- `compress_encoding` (optional): Compression encoding format, either empty string (`""`), `gzip` or `deflate` (default `gzip`).
Empty string means no compression
- `max_request_body_size` (optional): Max HTTP request body size in bytes before compression (if applied). By default `1_048_576` (1MB) is used.
- `metadata_attributes` (optional): List of regexes for attributes which should be send as metadata
- `log_format` (optional) (logs only): Format to use when sending logs to Sumo. (default `json`) (possible values: `json`, `text`)
- `metric_format` (optional) (metrics only): Format of the metrics to be sent (default is `prometheus`) (possible values: `carbon2`, `prometheus`)
  `carbon2` and `graphite` are going to be supported soon.
- `source_category` (optional): Desired source category. Useful if you want to override the source category configured for the source.
- `source_name` (optional): Desired source name. Useful if you want to override the source name configured for the source.
- `source_host` (optional): Desired host name. Useful if you want to override the source host configured for the source.
- `timeout` (default = 5s): Is the timeout for every attempt to send data to the backend.
Maximum connection timeout is 55s.
- `retry_on_failure`
  - `enabled` (default = true)
  - `initial_interval` (default = 5s): Time to wait after the first failure before retrying; ignored if `enabled` is `false`
  - `max_interval` (default = 30s): Is the upper bound on backoff; ignored if `enabled` is `false`
  - `max_elapsed_time` (default = 120s): Is the maximum amount of time spent trying to send a batch; ignored if `enabled` is `false`
- `sending_queue`
  - `enabled` (default = false)
  - `num_consumers` (default = 10): Number of consumers that dequeue batches; ignored if `enabled` is `false`
  - `queue_size` (default = 5000): Maximum number of batches kept in memory before data; ignored if `enabled` is `false`;
  User should calculate this as `num_seconds * requests_per_second` where:
    - `num_seconds` is the number of seconds to buffer in case of a backend outage
    - `requests_per_second` is the average number of requests per seconds.

Example:

```yaml
exporters:
  sumologic:
    endpoint: http://localhost:3000
    compress_encoding: "gzip"
    max_request_body_size: "1_048_576"  # 1MB
    log_format: "text"
    metric_format: "prometheus"
    source_category: "custom category"
    source_name: "custom name"
    source_host: "custom host"
    metadata_attributes:
      - k8s.*
```
