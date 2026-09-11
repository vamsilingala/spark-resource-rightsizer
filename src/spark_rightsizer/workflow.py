"""Application workflow from inventory discovery to normalized assessment rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional

from spark_rightsizer.connectors.base import Connector
from spark_rightsizer.domain import Execution
from spark_rightsizer.export import flatten_assessment
from spark_rightsizer.planner import CapacityPlanner
from spark_rightsizer.settings import Settings


def newest_eligible(executions: Iterable[Execution]) -> Optional[Execution]:
    candidates = [execution for execution in executions if execution.eligible]
    return max(candidates, key=lambda item: (item.finished_at or item.began_at, item.key), default=None)


def latest_execution_contexts(executions: Iterable[Execution]) -> List[Execution]:
    candidates = [execution for execution in executions if execution.eligible]
    latest = newest_eligible(candidates)
    if latest is None:
        return []
    logical_key = latest.attributes.get("logical_execution_key")
    if not logical_key:
        return [latest]
    return sorted(
        [item for item in candidates if item.attributes.get("logical_execution_key") == logical_key],
        key=lambda item: item.key,
    )


@dataclass
class AssessmentBatch:
    rows: List[Dict[str, Any]] = field(default_factory=list)
    issues: List[Dict[str, str]] = field(default_factory=list)
    workloads_seen: int = 0
    executions_selected: int = 0


class AssessmentWorkflow:
    def __init__(self, settings: Settings, connector: Connector) -> None:
        self.settings = settings
        self.connector = connector
        self.planner = CapacityPlanner(settings.policy)

    def execute(self) -> AssessmentBatch:
        result = AssessmentBatch()
        zone = self.settings.zone
        end = datetime.combine(self.settings.as_of + timedelta(days=1), time.min, tzinfo=zone)
        start = end - timedelta(days=self.settings.history_days)
        workloads = list(self.connector.workloads())
        if self.settings.workload_allowlist:
            allowed = set(self.settings.workload_allowlist)
            workloads = [item for item in workloads if item.label in allowed or item.key in allowed]
        if self.settings.limit:
            workloads = workloads[: self.settings.limit]
        result.workloads_seen = len(workloads)

        catalog = self.connector.shape_catalog()
        worker_choices = self.connector.worker_shape_choices()
        driver_choices = self.connector.driver_shape_choices()
        for workload in workloads:
            try:
                selected_contexts = latest_execution_contexts(
                    self.connector.executions(workload, start, end)
                )
                if not selected_contexts:
                    result.issues.append(
                        {"workload_key": workload.key, "phase": "selection", "message": "No eligible execution"}
                    )
                    continue
                result.executions_selected += len(selected_contexts)
                for selected in selected_contexts:
                    snapshot = self.connector.snapshot(workload, selected)
                    plan = self.planner.build(snapshot, catalog, worker_choices, driver_choices)
                    result.rows.append(flatten_assessment(snapshot, plan))
                    result.issues.extend(
                        {"workload_key": workload.key, "phase": "telemetry", "message": note}
                        for note in snapshot.notes
                    )
            except Exception as exc:
                result.issues.append(
                    {"workload_key": workload.key, "phase": "assessment", "message": str(exc)}
                )
        return result
