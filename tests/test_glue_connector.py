from datetime import datetime, timedelta, timezone

from spark_rightsizer.connectors.glue import GlueConnector, glue_shape_catalog
from spark_rightsizer.domain import Cloud, Runtime
from spark_rightsizer.settings import Settings, SourceSettings


class FakeGlue:
    def list_jobs(self, **kwargs):
        return {"JobNames": ["sample-job"]}

    def get_job(self, **kwargs):
        return {
            "Job": {"Role": "sample-role", "Command": {"Name": "glueetl"}, "GlueVersion": "5.0"}
        }

    def get_job_runs(self, **kwargs):
        began = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return {
            "JobRuns": [
                {
                    "Id": "run-1",
                    "StartedOn": began,
                    "CompletedOn": began + timedelta(minutes=10),
                    "JobRunState": "SUCCEEDED",
                    "WorkerType": "G.2X",
                    "NumberOfWorkers": 8,
                    "Arguments": {"--enable-auto-scaling": "true"},
                }
            ]
        }


class FakeCloudWatch:
    def get_metric_statistics(self, **kwargs):
        return {"Datapoints": []}


def _settings():
    return Settings(
        source=SourceSettings(Runtime.GLUE, Cloud.AWS, "us-east-1", {"autoscale_floor": 3}),
        as_of=datetime(2026, 1, 2, tzinfo=timezone.utc).date(),
    )


def test_glue_autoscaling_layout_is_normalized():
    connector = GlueConnector(_settings(), FakeGlue(), FakeCloudWatch())
    workload = list(connector.workloads())[0]
    run = list(
        connector.executions(
            workload,
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2027, 1, 1, tzinfo=timezone.utc),
        )
    )[0]
    assert run.eligible
    assert run.layout.uses_autoscaling
    assert run.layout.scale_floor == 3
    assert run.layout.scale_ceiling == 8
    assert run.layout.attributes["driver_worker_reserve"] == 1


def test_glue_catalog_uses_dpu_multiplier_for_configured_price():
    catalog = glue_shape_catalog(0.4)
    assert catalog["G.2X"].price_per_hour == 0.8
    assert catalog["R.4X"].ram_gib == 128
