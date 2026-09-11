"""Databricks telemetry connector for AWS, Azure, and Google Cloud workspaces."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from spark_rightsizer.connectors.base import Connector
from spark_rightsizer.domain import (
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
from spark_rightsizer.storage import read_event_locations

_SQL_OBJECT = re.compile(r"^[A-Za-z0-9_.]+$")
_COMPUTE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")


def _plain(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    converter = getattr(value, "as_dict", None)
    if callable(converter):
        return dict(converter())
    return {key: item for key, item in vars(value).items() if not key.startswith("_")} if hasattr(value, "__dict__") else {}


def _instant(milliseconds: Any) -> Optional[datetime]:
    if milliseconds in (None, 0, ""):
        return None
    return datetime.fromtimestamp(float(milliseconds) / 1000.0, tz=timezone.utc)


def _run_state(value: Any) -> ExecutionState:
    key = str(value or "").upper()
    if key == "SUCCESS":
        return ExecutionState.SUCCESS
    if key in {"FAILED", "TIMEDOUT", "TIMED_OUT"}:
        return ExecutionState.FAILURE
    if key in {"CANCELED", "CANCELLED"}:
        return ExecutionState.STOPPED
    if key in {"RUNNING", "PENDING", "QUEUED"}:
        return ExecutionState.ACTIVE
    return ExecutionState.UNKNOWN


class DatabricksConnector(Connector):
    def __init__(self, settings: Settings, workspace: Any = None) -> None:
        self.settings = settings
        if workspace is not None:
            self.workspace = workspace
            return
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as exc:
            raise RuntimeError("Install the databricks package extra") from exc
        options: Dict[str, Any] = {}
        parameters = settings.source.parameters
        if parameters.get("host"):
            options["host"] = parameters["host"]
        if parameters.get("profile"):
            options["profile"] = parameters["profile"]
        self.workspace = WorkspaceClient(**options)

    def workloads(self) -> Iterable[Workload]:
        scheduled_only = bool(self.settings.source.parameters.get("scheduled_only", True))
        for item in self.workspace.jobs.list(expand_tasks=False):
            raw = _plain(item)
            definition = _plain(raw.get("settings"))
            key = str(raw.get("job_id") or "")
            trigger = definition.get("schedule") or definition.get("trigger") or definition.get("continuous")
            if not key or (scheduled_only and not trigger):
                continue
            yield Workload(
                key=key,
                label=str(definition.get("name") or key),
                principal=str(raw.get("run_as_user_name") or raw.get("creator_user_name") or ""),
                annotations={"trigger": str(trigger or "")},
            )

    def executions(self, workload: Workload, start: datetime, end: datetime) -> Iterable[Execution]:
        summaries = self.workspace.jobs.list_runs(
            job_id=int(workload.key),
            completed_only=True,
            expand_tasks=True,
            start_time_from=int(start.timestamp() * 1000),
            start_time_to=int(end.timestamp() * 1000),
        )
        for item in summaries:
            summary = _plain(item)
            run_key = str(summary.get("run_id") or "")
            if not run_key:
                continue
            try:
                detail = _plain(self.workspace.jobs.get_run(int(run_key)))
            except Exception:
                detail = summary
            began = _instant(detail.get("start_time") or summary.get("start_time"))
            if began is None:
                continue
            finished = _instant(detail.get("end_time") or summary.get("end_time"))
            state = _run_state(_plain(detail.get("state")).get("result_state"))
            tasks = [_plain(task) for task in detail.get("tasks") or []]
            cluster_keys = {
                str(_plain(task.get("cluster_instance")).get("cluster_id"))
                for task in tasks
                if _plain(task.get("cluster_instance")).get("cluster_id")
            }
            top_level_key = _plain(detail.get("cluster_instance")).get("cluster_id")
            if top_level_key:
                cluster_keys.add(str(top_level_key))
            task_states = [_run_state(_plain(task.get("state")).get("result_state")) for task in tasks]
            repeated = any(int(task.get("attempt_number") or 0) > 0 for task in tasks)
            recovered = bool(detail.get("repair_history") or detail.get("repair_id") or repeated)
            for cluster_key in sorted(cluster_keys or {""}):
                yield Execution(
                    key=f"{run_key}@{cluster_key}" if cluster_key else run_key,
                    workload_key=workload.key,
                    state=state,
                    began_at=began,
                    finished_at=finished,
                    attempt_index=int(detail.get("attempt_number") or 0),
                    recovered=recovered,
                    context_complete=bool(cluster_key) and all(value == ExecutionState.SUCCESS for value in task_states),
                    attributes={
                        "logical_execution_key": run_key,
                        "run_key": run_key,
                        "cluster_key": cluster_key,
                    },
                )

    def snapshot(self, workload: Workload, execution: Execution) -> Snapshot:
        cluster_key = str(execution.attributes.get("cluster_key") or "")
        if not cluster_key:
            raise ValueError("The Databricks execution does not identify job compute")
        cluster = _plain(self.workspace.clusters.get(cluster_key))
        autoscaling = _plain(cluster.get("autoscale"))
        layout = ClusterLayout(
            cloud=self.settings.source.cloud,
            runtime=Runtime.DATABRICKS,
            region=self.settings.source.region,
            worker_shape=str(cluster.get("node_type_id") or ""),
            fixed_workers=int(cluster["num_workers"]) if cluster.get("num_workers") is not None else None,
            scale_floor=int(autoscaling["min_workers"]) if autoscaling.get("min_workers") is not None else None,
            scale_ceiling=int(autoscaling["max_workers"]) if autoscaling.get("max_workers") is not None else None,
            driver_shape=str(cluster.get("driver_node_type_id") or cluster.get("node_type_id") or ""),
            platform_minimum=1,
            attributes={"cluster_key": cluster_key, "runtime_version": cluster.get("spark_version")},
        )
        execution.layout = layout
        workers, driver, notes = self._node_signals(cluster_key, execution.began_at, execution.finished_at or execution.began_at)
        spark = SparkEvidence()
        event_locations = self._event_locations(workload, execution)
        if event_locations:
            spark, event_notes = read_event_locations(event_locations)
            notes.extend(event_notes)
        return Snapshot(workload, execution, layout, workers, driver, spark, notes)

    def shape_catalog(self) -> Dict[str, MachineShape]:
        return dict(self.settings.shapes)

    def worker_shape_choices(self) -> Sequence[str]:
        return self.settings.allowed_worker_shapes or tuple(self.settings.shapes)

    def driver_shape_choices(self) -> Sequence[str]:
        return self.settings.allowed_driver_shapes or tuple(self.settings.shapes)

    def _event_locations(self, workload: Workload, execution: Execution) -> List[str]:
        template = self.settings.source.parameters.get("event_log_location")
        if not template:
            return []
        templates = template if isinstance(template, list) else [template]
        return [
            str(item).format(
                workload_key=workload.key,
                workload_label=workload.label,
                execution_key=execution.attributes.get("run_key", execution.key),
                cluster_key=execution.attributes.get("cluster_key", ""),
            )
            for item in templates
        ]

    def _node_signals(
        self, cluster_key: str, began: datetime, finished: datetime
    ) -> Tuple[HostSignals, HostSignals, List[str]]:
        table = str(self.settings.source.parameters.get("node_metrics_table", "system.compute.node_timeline"))
        if not _SQL_OBJECT.fullmatch(table) or not _COMPUTE_KEY.fullmatch(cluster_key):
            return HostSignals(), HostSignals(), ["Rejected unsafe telemetry identifier"]
        began_text = began.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        finished_text = finished.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        statement = f"""
            SELECT driver,
                   COUNT(*),
                   percentile_approx(cpu_user_percent + cpu_system_percent, 0.95),
                   MAX(cpu_user_percent + cpu_system_percent),
                   percentile_approx(mem_used_percent, 0.95),
                   MAX(mem_used_percent),
                   percentile_approx(cpu_wait_percent, 0.95)
              FROM {table}
             WHERE cluster_id = '{cluster_key}'
               AND start_time < TIMESTAMP '{finished_text}'
               AND end_time > TIMESTAMP '{began_text}'
             GROUP BY driver
        """
        try:
            rows = self._query(statement)
        except Exception as exc:
            return HostSignals(), HostSignals(), ["Databricks node telemetry failed: " + str(exc)]
        workers = HostSignals(origin=table)
        driver = HostSignals(origin=table)
        for values in rows:
            row = list(values)
            target = driver if bool(row[0]) else workers
            target.observations = int(row[1] or 0)
            target.cpu_q95 = float(row[2]) if row[2] is not None else None
            target.cpu_max = float(row[3]) if row[3] is not None else None
            target.ram_q95 = float(row[4]) if row[4] is not None else None
            target.ram_max = float(row[5]) if row[5] is not None else None
            target.io_wait_q95 = float(row[6]) if row[6] is not None else None
        return workers, driver, []

    def _query(self, statement: str) -> List[Sequence[Any]]:
        http_path = self.settings.source.parameters.get("sql_http_path")
        if http_path:
            try:
                from databricks import sql
            except ImportError as exc:
                raise RuntimeError("Install databricks-sql-connector") from exc
            with sql.connect(**self._sql_options(str(http_path))) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(statement)
                    return list(cursor.fetchall())
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise RuntimeError("Set sql_http_path or run inside an active Spark session") from exc
        session = SparkSession.getActiveSession()
        if session is None:
            raise RuntimeError("Set sql_http_path or run inside an active Spark session")
        return [tuple(row) for row in session.sql(statement).collect()]

    def _sql_options(self, http_path: str) -> Dict[str, Any]:
        sdk = getattr(self.workspace, "config", None)
        hostname = re.sub(r"^https?://", "", str(getattr(sdk, "host", ""))).rstrip("/")
        authenticate = getattr(sdk, "authenticate", None)
        if not hostname or not callable(authenticate):
            raise RuntimeError("Databricks unified authentication is unavailable")
        return {
            "server_hostname": hostname,
            "http_path": http_path,
            "credentials_provider": lambda: authenticate,
        }
