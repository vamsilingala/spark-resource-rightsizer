# Architecture

The package has three boundaries:

```text
platform connector -> normalized snapshot -> capacity planner -> report
```

Connectors own API pagination, authentication integration, and translation of platform fields. The normalized
snapshot contains only portable workload, execution, layout, host-signal, and Spark-evidence contracts. The
planner consumes those contracts and never calls a cloud API.

Spark listener logs are processed as a stream. Local paths and remote object-store URIs pass through the same
storage layer, including gzip, bzip2, and xz decoding. The parser records incomplete evidence when any selected
object cannot be read.

To add a runtime, implement `Connector` in `connectors/base.py`, return the neutral domain objects, and add a
factory branch. A connector must not embed customer names, fixed prices, credentials, or recommendation rules.
