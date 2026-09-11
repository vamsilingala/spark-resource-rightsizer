from datetime import datetime, timedelta, timezone

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
from spark_rightsizer.planner import CapacityPlanner


def _snapshot(workers=10, q95=4.0, peak=5, attributes=None):
    began = datetime(2026, 1, 1, tzinfo=timezone.utc)
    layout = ClusterLayout(
        cloud=Cloud.AWS,
        runtime=Runtime.FILE,
        region="sample",
        worker_shape="large",
        driver_shape="large",
        fixed_workers=workers,
        attributes=attributes or {},
    )
    execution = Execution(
        key="run",
        workload_key="job",
        state=ExecutionState.SUCCESS,
        began_at=began,
        finished_at=began + timedelta(minutes=10),
        layout=layout,
    )
    signals = HostSignals(observations=30, cpu_q95=20, cpu_max=40, ram_q95=25, ram_max=45)
    return Snapshot(
        workload=Workload("job", "job"),
        execution=execution,
        layout=layout,
        workers=signals,
        driver=signals,
        spark=SparkEvidence(
            is_complete=True,
            tasks=100,
            executor_compute_ms=1000,
            jvm_gc_ms=20,
            bytes_input=1000,
            executor_count_q95=q95,
            executor_count_max=peak,
        ),
    )


def _catalog():
    return {
        "large": MachineShape("large", 8, 32, price_per_hour=1.0, cpu_arch="x86"),
        "small": MachineShape("small", 4, 16, price_per_hour=0.5, cpu_arch="x86"),
    }


def test_fixed_worker_change_is_limited_to_one_policy_step():
    plan = CapacityPlanner().build(_snapshot(), _catalog(), ["small"], [])
    assert plan.fixed_workers == 7
    assert plan.worker_shape is None
    assert "set_fixed_worker_count" in plan.operations
    assert plan.projected_saving_percent == 27.27


def test_shape_can_change_when_quantity_stays_constant():
    plan = CapacityPlanner().build(_snapshot(workers=4, q95=3.0, peak=4), _catalog(), ["small"], [])
    assert plan.fixed_workers is None
    assert plan.worker_shape == "small"
    assert "change_worker_shape" in plan.operations


def test_failed_tasks_block_worker_reduction():
    snapshot = _snapshot()
    snapshot.spark.failed_tasks = 1
    plan = CapacityPlanner().build(snapshot, _catalog(), ["small"], [])
    assert plan.fixed_workers is None
    assert "failed tasks" in plan.capacity_explanation


def test_glue_executor_count_is_translated_to_allocated_workers():
    snapshot = _snapshot(
        workers=10,
        q95=8.0,
        peak=8,
        attributes={"executors_per_worker": 2, "driver_worker_reserve": 1},
    )
    plan = CapacityPlanner().build(snapshot, _catalog(), [], [])
    assert plan.fixed_workers == 7
