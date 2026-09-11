from datetime import datetime, timedelta, timezone

from spark_rightsizer.connectors.base import Connector
from spark_rightsizer.domain import (
    Cloud,
    ClusterLayout,
    Execution,
    ExecutionState,
    HostSignals,
    MachineShape,
    Runtime,
    Snapshot,
    SparkEvidence,
    Workload,
)
from spark_rightsizer.settings import Settings, SourceSettings
from spark_rightsizer.workflow import AssessmentWorkflow, newest_eligible


class FakeConnector(Connector):
    def __init__(self):
        self.workload = Workload("workload", "sample")
        began = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.layout = ClusterLayout(Cloud.AWS, Runtime.FILE, "sample", "large", fixed_workers=4)
        self.runs = [
            Execution("old", "workload", ExecutionState.SUCCESS, began, began + timedelta(minutes=4), layout=self.layout),
            Execution(
                "new",
                "workload",
                ExecutionState.SUCCESS,
                began + timedelta(days=1),
                began + timedelta(days=1, minutes=3),
                layout=self.layout,
            ),
        ]

    def workloads(self):
        return [self.workload]

    def executions(self, workload, start, end):
        return self.runs

    def snapshot(self, workload, execution):
        signals = HostSignals(observations=20, cpu_q95=30, cpu_max=50, ram_q95=30, ram_max=50)
        spark = SparkEvidence(is_complete=True, tasks=10, executor_count_q95=3, executor_count_max=4)
        return Snapshot(workload, execution, self.layout, signals, HostSignals(), spark)

    def shape_catalog(self):
        return {"large": MachineShape("large", 8, 32)}

    def worker_shape_choices(self):
        return []

    def driver_shape_choices(self):
        return []


def test_newest_eligible_prefers_fresh_evidence():
    connector = FakeConnector()
    assert newest_eligible(connector.runs).key == "new"


def test_workflow_produces_provider_neutral_row():
    settings = Settings(
        source=SourceSettings(Runtime.FILE, Cloud.AWS, "sample"),
        as_of=datetime(2026, 1, 3, tzinfo=timezone.utc).date(),
        history_days=10,
    )
    result = AssessmentWorkflow(settings, FakeConnector()).execute()
    assert result.workloads_seen == 1
    assert result.executions_selected == 1
    assert result.rows[0]["execution_key"] == "new"
    assert result.rows[0]["runtime"] == "file"
