"""AWS Glue Spark connector backed by Glue and CloudWatch APIs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
from spark_rightsizer.settings import Settings
from spark_rightsizer.statistics import percent_value, quantile
from spark_rightsizer.storage import read_event_locations


def glue_shape_catalog(dpu_price: Optional[float] = None) -> Dict[str, MachineShape]:
    specifications = {
        "Standard": (4, 16, 50, 1),
        "G.025X": (2, 4, 84, 0.25),
        "G.1X": (4, 16, 94, 1),
        "G.2X": (8, 32, 138, 2),
        "G.4X": (16, 64, 256, 4),
        "G.8X": (32, 128, 512, 8),
        "G.12X": (48, 192, 768, 12),
        "G.16X": (64, 256, 1024, 16),
        "R.1X": (4, 32, 94, 1),
        "R.2X": (8, 64, 138, 2),
        "R.4X": (16, 128, 256, 4),
        "R.8X": (32, 256, 512, 8),
    }
    return {
        key: MachineShape(
            key=key,
            cores=float(cores),
            ram_gib=float(ram),
            scratch_gib=float(disk),
            price_per_hour=(units * dpu_price if dpu_price is not None else None),
            cpu_arch="managed",
            units=float(units),
        )
        for key, (cores, ram, disk, units) in specifications.items()
    }


def _aware(value: Any) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _execution_state(value: Any) -> ExecutionState:
    key = str(value or "").upper()
    if key == "SUCCEEDED":
        return ExecutionState.SUCCESS
    if key in {"FAILED", "ERROR", "TIMEOUT"}:
        return ExecutionState.FAILURE
    if key == "STOPPED":
        return ExecutionState.STOPPED
    if key in {"RUNNING", "STARTING", "WAITING"}:
        return ExecutionState.ACTIVE
    return ExecutionState.UNKNOWN


class GlueConnector(Connector):
    def __init__(
        self,
        settings: Settings,
        glue_api: Any = None,
        cloudwatch_api: Any = None,
    ) -> None:
        self.settings = settings
        if glue_api is not None and cloudwatch_api is not None:
            self.glue = glue_api
            self.cloudwatch = cloudwatch_api
            return
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("Install the aws-glue package extra") from exc
        session_options: Dict[str, Any] = {"region_name": settings.source.region}
        profile = settings.source.parameters.get("aws_profile")
        if profile:
            session_options["profile_name"] = profile
        session = boto3.Session(**session_options)
        self.glue = glue_api or session.client("glue")
        self.cloudwatch = cloudwatch_api or session.client("cloudwatch")

    def workloads(self) -> Iterable[Workload]:
        token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"MaxResults": 1000}
            if token:
                request["NextToken"] = token
            page = self.glue.list_jobs(**request)
            for name in page.get("JobNames") or []:
                definition = self.glue.get_job(JobName=name).get("Job") or {}
                command = str((definition.get("Command") or {}).get("Name") or "")
                if command and command not in {"glueetl", "gluestreaming"}:
                    continue
                yield Workload(
                    key=str(name),
                    label=str(name),
                    principal=str(definition.get("Role") or ""),
                    annotations={"glue_version": definition.get("GlueVersion")},
                )
            token = page.get("NextToken")
            if not token:
                break

    def executions(self, workload: Workload, start: datetime, end: datetime) -> Iterable[Execution]:
        token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {"JobName": workload.label, "MaxResults": 200}
            if token:
                request["NextToken"] = token
            page = self.glue.get_job_runs(**request)
            for raw in page.get("JobRuns") or []:
                began = _aware(raw.get("StartedOn"))
                finished = _aware(raw.get("CompletedOn"))
                if began is None or began >= end or (finished is not None and finished < start):
                    continue
                arguments = dict(raw.get("Arguments") or {})
                autoscaling = str(arguments.get("--enable-auto-scaling", "")).strip().casefold() == "true"
                shape = str(raw.get("WorkerType") or "Standard")
                count_value = raw.get("NumberOfWorkers", raw.get("MaxCapacity"))
                count = int(count_value) if count_value is not None else None
                minimum = int(self.settings.source.parameters.get("autoscale_floor", 2))
                layout = ClusterLayout(
                    cloud=Cloud.AWS,
                    runtime=Runtime.GLUE,
                    region=self.settings.source.region,
                    worker_shape=shape,
                    fixed_workers=None if autoscaling else count,
                    scale_floor=minimum if autoscaling else None,
                    scale_ceiling=count if autoscaling else None,
                    platform_minimum=minimum,
                    attributes={
                        "glue_version": raw.get("GlueVersion"),
                        "execution_class": raw.get("ExecutionClass"),
                        "executors_per_worker": 2 if shape == "Standard" else 1,
                        "driver_worker_reserve": 1,
                    },
                )
                yield Execution(
                    key=str(raw.get("Id") or ""),
                    workload_key=workload.key,
                    state=_execution_state(raw.get("JobRunState")),
                    began_at=began,
                    finished_at=finished,
                    attempt_index=int(raw.get("Attempt") or 0),
                    layout=layout,
                    attributes={
                        "logical_execution_key": str(raw.get("Id") or ""),
                        "arguments": arguments,
                        "dpu_seconds": raw.get("DPUSeconds"),
                    },
                )
            token = page.get("NextToken")
            if not token:
                break

    def snapshot(self, workload: Workload, execution: Execution) -> Snapshot:
        if execution.layout is None:
            raise ValueError("The Glue execution has no worker layout")
        workers, driver, notes, glue_skew = self._host_signals(workload, execution)
        spark = SparkEvidence()
        event_locations = self._event_locations(workload, execution)
        if event_locations:
            spark, event_notes = read_event_locations(event_locations)
            notes.extend(event_notes)
        if glue_skew and "platform_skew_signal" not in spark.hazards:
            spark.hazards.append("platform_skew_signal")
        return Snapshot(workload, execution, execution.layout, workers, driver, spark, notes)

    def shape_catalog(self) -> Dict[str, MachineShape]:
        configured_price = self.settings.source.parameters.get("dpu_price_per_hour")
        catalog = glue_shape_catalog(float(configured_price) if configured_price is not None else None)
        catalog.update(self.settings.shapes)
        return catalog

    def worker_shape_choices(self) -> Sequence[str]:
        if self.settings.allowed_worker_shapes:
            return [key for key in self.settings.allowed_worker_shapes if key != "Standard"]
        return tuple(key for key in self.shape_catalog() if key not in {"Standard", "G.025X"})

    def driver_shape_choices(self) -> Sequence[str]:
        return ()

    def _event_locations(self, workload: Workload, execution: Execution) -> List[str]:
        template = self.settings.source.parameters.get("event_log_location")
        if not template:
            return []
        templates = template if isinstance(template, list) else [template]
        return [
            str(item).format(
                workload_key=workload.key,
                workload_label=workload.label,
                execution_key=execution.key,
            )
            for item in templates
        ]

    def _host_signals(
        self, workload: Workload, execution: Execution
    ) -> Tuple[HostSignals, HostSignals, List[str], bool]:
        end = execution.finished_at or datetime.now(timezone.utc)
        notes: List[str] = []

        def series(metric: str, group: Optional[str] = None) -> List[float]:
            try:
                return self._metric_series(workload.label, execution.key, metric, execution.began_at, end, group)
            except Exception as exc:
                notes.append(f"CloudWatch {metric}: {exc}")
                return []

        worker_cpu = [value for value in (percent_value(item) for item in series("glue.ALL.system.cpuSystemLoad")) if value is not None]
        worker_ram = series("glue.ALL.memory.total.used.percentage", "resource_utilization")
        worker_allocation = series("glue.driver.workerUtilization", "resource_utilization")
        driver_cpu = [value for value in (percent_value(item) for item in series("glue.driver.system.cpuSystemLoad")) if value is not None]
        driver_ram = [value for value in (percent_value(item) for item in series("glue.driver.jvm.heap.usage")) if value is not None]
        skew = series("glue.driver.skewness.stage", "job_performance")
        workers = HostSignals(
            observations=max(len(worker_cpu), len(worker_ram), len(worker_allocation)),
            cpu_q95=quantile(worker_cpu, 0.95),
            cpu_max=max(worker_cpu) if worker_cpu else None,
            ram_q95=quantile(worker_ram, 0.95),
            ram_max=max(worker_ram) if worker_ram else None,
            allocation_q95=quantile(worker_allocation, 0.95),
            origin="AWS Glue CloudWatch",
        )
        driver = HostSignals(
            observations=max(len(driver_cpu), len(driver_ram)),
            cpu_q95=quantile(driver_cpu, 0.95),
            cpu_max=max(driver_cpu) if driver_cpu else None,
            ram_q95=quantile(driver_ram, 0.95),
            ram_max=max(driver_ram) if driver_ram else None,
            origin="AWS Glue CloudWatch",
        )
        return workers, driver, notes, any(value > 0 for value in skew)

    def _metric_series(
        self,
        workload_label: str,
        execution_key: str,
        metric: str,
        start: datetime,
        end: datetime,
        group: Optional[str],
    ) -> List[float]:
        seconds = max(60, int((end - start).total_seconds()))
        period = max(60, ((seconds // 1400) + 1) * 60)
        dimensions = [
            {"Name": "JobName", "Value": workload_label},
            {"Name": "JobRunId", "Value": execution_key},
            {"Name": "Type", "Value": "gauge"},
        ]
        if group:
            dimensions.append({"Name": "ObservabilityGroup", "Value": group})
        response = self.cloudwatch.get_metric_statistics(
            Namespace="Glue",
            MetricName=metric,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=end,
            Period=period,
            Statistics=["Average"],
        )
        return [
            float(point["Average"])
            for point in response.get("Datapoints") or []
            if point.get("Average") is not None
        ]
