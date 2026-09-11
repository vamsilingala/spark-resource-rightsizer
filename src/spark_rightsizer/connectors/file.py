"""Offline connector for synthetic or previously normalized snapshots."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

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


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class OfflineConnector(Connector):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        filename = settings.source.parameters.get("snapshot_file")
        if not filename:
            raise ValueError("source.parameters.snapshot_file is required")
        document = json.loads(Path(str(filename)).read_text(encoding="utf-8"))
        self.records: List[Dict[str, Any]] = list(document.get("snapshots") or [])

    def workloads(self) -> Iterable[Workload]:
        seen = set()
        for record in self.records:
            raw = record.get("workload") or {}
            key = str(raw.get("key", ""))
            if key and key not in seen:
                seen.add(key)
                yield Workload(
                    key=key,
                    label=str(raw.get("label") or key),
                    principal=str(raw.get("principal") or ""),
                )

    def executions(self, workload: Workload, start: datetime, end: datetime) -> Iterable[Execution]:
        for record in self.records:
            raw_workload = record.get("workload") or {}
            if str(raw_workload.get("key")) != workload.key:
                continue
            raw = record.get("execution") or {}
            began = _timestamp(str(raw["began_at"]))
            finished = _timestamp(str(raw["finished_at"])) if raw.get("finished_at") else None
            if began >= end or (finished is not None and finished < start):
                continue
            yield Execution(
                key=str(raw["key"]),
                workload_key=workload.key,
                state=ExecutionState(str(raw.get("state", "unknown"))),
                began_at=began,
                finished_at=finished,
                attempt_index=int(raw.get("attempt_index", 0)),
                recovered=bool(raw.get("recovered", False)),
                context_complete=bool(raw.get("context_complete", True)),
                layout=self._layout(record.get("layout") or {}),
                attributes={"record": record},
            )

    def snapshot(self, workload: Workload, execution: Execution) -> Snapshot:
        record = execution.attributes["record"]
        layout = execution.layout
        if layout is None:
            raise ValueError("Offline execution is missing a layout")
        return Snapshot(
            workload=workload,
            execution=execution,
            layout=layout,
            workers=HostSignals(**(record.get("worker_signals") or {})),
            driver=HostSignals(**(record.get("driver_signals") or {})),
            spark=SparkEvidence(**(record.get("spark_evidence") or {})),
            notes=[str(item) for item in record.get("notes") or []],
        )

    def shape_catalog(self) -> Dict[str, MachineShape]:
        return dict(self.settings.shapes)

    def worker_shape_choices(self) -> Sequence[str]:
        return self.settings.allowed_worker_shapes or tuple(self.settings.shapes)

    def driver_shape_choices(self) -> Sequence[str]:
        return self.settings.allowed_driver_shapes or tuple(self.settings.shapes)

    def _layout(self, raw: Dict[str, Any]) -> ClusterLayout:
        return ClusterLayout(
            cloud=Cloud(str(raw.get("cloud", self.settings.source.cloud.value))),
            runtime=Runtime(str(raw.get("runtime", Runtime.FILE.value))),
            region=str(raw.get("region", self.settings.source.region)),
            worker_shape=str(raw["worker_shape"]),
            fixed_workers=(int(raw["fixed_workers"]) if raw.get("fixed_workers") is not None else None),
            scale_floor=(int(raw["scale_floor"]) if raw.get("scale_floor") is not None else None),
            scale_ceiling=(int(raw["scale_ceiling"]) if raw.get("scale_ceiling") is not None else None),
            driver_shape=str(raw["driver_shape"]) if raw.get("driver_shape") else None,
            platform_minimum=int(raw.get("platform_minimum", 1)),
            attributes=dict(raw.get("attributes") or {}),
        )
