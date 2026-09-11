# Spark Job Rightsizer

[![CI](https://github.com/vamsilingala/spark-job-rightsizer/actions/workflows/ci.yml/badge.svg)](https://github.com/vamsilingala/spark-job-rightsizer/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Spark Job Rightsizer is a provider-neutral toolkit for reviewing Apache Spark capacity. It converts runtime
inventory, host telemetry, and standard Spark listener events into a common snapshot, then proposes one
bounded configuration trial.

This release is an independent implementation built around public platform interfaces. It contains no
customer source code, tenant identifiers, credentials, workspace paths, internal job names, or embedded
regional prices. See [PROVENANCE.md](PROVENANCE.md) for the maintenance rules.

## Supported sources

| Source | Cloud | Direct inventory | Host telemetry | Spark event logs |
|---|---|---:|---:|---:|
| Databricks | AWS, Azure, GCP | Yes | Compute system table | Optional |
| AWS Glue Spark | AWS | Yes | CloudWatch | Optional |
| Normalized snapshot | Any | File | Included in file | Included in file |

The recommendation planner has no vendor-specific branches. Connectors translate each platform into the
contracts in `domain.py`.

## What the planner can propose

- a smaller fixed worker allocation;
- a different autoscaling floor or ceiling;
- an approved worker shape;
- an approved driver shape when the platform exposes one;
- a price-based projection only when the caller supplies prices.

The planner changes worker quantity and worker shape in separate trials. It refuses worker reductions when
Spark evidence is incomplete, host sampling is insufficient, or failure, pressure, spill, garbage collection,
or skew signals cross the configured policy.

## Local demonstration

```bash
git clone git@github.com:vamsilingala/spark-job-rightsizer.git
cd spark-job-rightsizer
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
spark-rightsizer --config configs/file.example.json
```

The sample data is generated and uses fictional identifiers and shapes.

Python 3.11 or newer is required.

## Cloud installation

```bash
pip install -e '.[databricks,aws-files]'    # Databricks with S3 logs
pip install -e '.[databricks,azure-files]'  # Databricks with ADLS logs
pip install -e '.[databricks,gcp-files]'    # Databricks with GCS logs
pip install -e '.[aws-glue,aws-files]'      # AWS Glue Spark
```

Authentication comes from the Databricks or AWS SDK credential chain. Do not place secrets in configuration
files.

## Configuration

Configuration has five independent parts:

- `source`: connector, cloud, region, and connector parameters;
- `window`: assessment end date, history length, and timezone;
- `policy`: explicit safety and headroom values;
- `shapes`: caller-provided hardware and optional price data;
- `allowed_worker_shapes` and `allowed_driver_shapes`: rollout allowlists.

See the files in [`configs`](configs) for complete examples. The workflow chooses the newest eligible
successful execution in the window so the assessment favors current workload behavior. Retries, recovered
runs, incomplete contexts, and unsuccessful executions are not candidates.

## Repository map

```text
src/spark_rightsizer/
├── domain.py             # neutral contracts
├── settings.py           # configuration and policy
├── event_stream.py       # public Spark listener-event aggregation
├── storage.py            # local/S3/ADLS/GCS log access
├── planner.py            # vendor-independent trial planning
├── workflow.py           # orchestration
├── export.py             # CSV and JSON reports
└── connectors/           # Databricks, Glue, and offline adapters
```

## Validation

```bash
pytest
ruff check .
python -m compileall -q src tests
```

Recommendations are advisory. Test a proposal against an agreed performance and cost baseline before
changing production capacity.

## License

Apache License 2.0. See [LICENSE](LICENSE).
