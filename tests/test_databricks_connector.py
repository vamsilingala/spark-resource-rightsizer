from datetime import datetime, timezone
from types import SimpleNamespace

from spark_rightsizer.connectors.databricks import DatabricksConnector
from spark_rightsizer.domain import Cloud, Runtime, Workload
from spark_rightsizer.settings import Settings, SourceSettings


class FakeJobs:
    def list(self, **kwargs):
        return [
            {
                "job_id": 11,
                "creator_user_name": "sample-user",
                "settings": {"name": "scheduled", "schedule": {"pause_status": "UNPAUSED"}},
            }
        ]

    def list_runs(self, **kwargs):
        return [{"run_id": 21, "start_time": 1000, "end_time": 3000}]

    def get_run(self, run_id):
        return {
            "run_id": run_id,
            "start_time": 1000,
            "end_time": 3000,
            "state": {"result_state": "SUCCESS"},
            "tasks": [
                {
                    "attempt_number": 0,
                    "state": {"result_state": "SUCCESS"},
                    "cluster_instance": {"cluster_id": "cluster-1"},
                }
            ],
        }


class FakeWorkspace:
    def __init__(self):
        self.jobs = FakeJobs()
        self.config = SimpleNamespace(host="https://sample.cloud.databricks.com", authenticate=lambda: {})


def _settings(parameters=None):
    return Settings(
        source=SourceSettings(Runtime.DATABRICKS, Cloud.AZURE, "sample", parameters or {}),
        as_of=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
    )


def test_jobs_and_clean_cluster_execution_are_normalized():
    connector = DatabricksConnector(_settings(), workspace=FakeWorkspace())
    workload = list(connector.workloads())[0]
    runs = list(
        connector.executions(
            workload,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
            datetime(2030, 1, 1, tzinfo=timezone.utc),
        )
    )
    assert workload == Workload("11", "scheduled", "sample-user", {"trigger": "{'pause_status': 'UNPAUSED'}"})
    assert runs[0].eligible
    assert runs[0].attributes["cluster_key"] == "cluster-1"


def test_sql_connector_reuses_sdk_authentication():
    connector = DatabricksConnector(_settings(), workspace=FakeWorkspace())
    options = connector._sql_options("/sql/1.0/warehouses/sample")
    assert options["server_hostname"] == "sample.cloud.databricks.com"
    assert callable(options["credentials_provider"]())
