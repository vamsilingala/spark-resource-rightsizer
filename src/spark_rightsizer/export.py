"""Stable report records and crash-safe local output."""

from __future__ import annotations

import csv
import json
import os
import tempfile
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from spark_rightsizer.domain import ChangePlan, Snapshot


def json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_value(item) for item in value]
    return value


def flatten_assessment(snapshot: Snapshot, plan: ChangePlan) -> Dict[str, Any]:
    layout = snapshot.layout
    spark = snapshot.spark
    return {
        "runtime": layout.runtime.value,
        "cloud": layout.cloud.value,
        "region": layout.region,
        "workload_key": snapshot.workload.key,
        "workload_label": snapshot.workload.label,
        "principal": snapshot.workload.principal,
        "execution_key": snapshot.execution.key,
        "execution_began_at": snapshot.execution.began_at.isoformat(),
        "execution_minutes": round(snapshot.execution.elapsed_seconds / 60.0, 2),
        "existing_fixed_workers": layout.fixed_workers,
        "existing_scale_floor": layout.scale_floor,
        "existing_scale_ceiling": layout.scale_ceiling,
        "existing_worker_shape": layout.worker_shape,
        "existing_driver_shape": layout.driver_shape,
        "proposed_fixed_workers": plan.fixed_workers,
        "proposed_scale_floor": plan.scale_floor,
        "proposed_scale_ceiling": plan.scale_ceiling,
        "proposed_worker_shape": plan.worker_shape,
        "proposed_driver_shape": plan.driver_shape,
        "operations": ",".join(plan.operations),
        "assurance": plan.assurance,
        "capacity_explanation": plan.capacity_explanation,
        "worker_shape_explanation": plan.worker_shape_explanation,
        "driver_shape_explanation": plan.driver_shape_explanation,
        "projected_saving_percent": plan.projected_saving_percent,
        "saving_scope": plan.saving_scope,
        "worker_cpu_q95": snapshot.workers.cpu_q95,
        "worker_ram_q95": snapshot.workers.ram_q95,
        "driver_cpu_q95": snapshot.driver.cpu_q95,
        "driver_ram_q95": snapshot.driver.ram_q95,
        "spark_evidence_complete": spark.is_complete,
        "spark_tasks": spark.tasks,
        "spark_failed_tasks": spark.failed_tasks,
        "executor_count_q95": spark.executor_count_q95,
        "executor_count_max": spark.executor_count_max,
        "shuffle_gib": round(spark.shuffle_gib, 3),
        "spill_to_input": round(spark.spill_to_input, 4),
        "gc_share": round(spark.gc_share, 4),
        "stage_skew_q95_median": spark.stage_skew_q95_median,
        "hazards": ",".join(spark.hazards),
        "notes": " | ".join(snapshot.notes),
    }


def _replace_file(destination: Path, write: Any) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=destination.name + ".", dir=str(destination.parent))
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        write(temporary)
        os.replace(str(temporary), str(destination))
    finally:
        if temporary.exists():
            temporary.unlink()


def write_assessments(rows: Iterable[Mapping[str, Any]], directory: str, timestamp: str) -> Dict[str, str]:
    values = [dict(row) for row in rows]
    root = Path(directory)
    json_path = root / f"spark_assessment_{timestamp}.json"
    csv_path = root / f"spark_assessment_{timestamp}.csv"

    def write_json(path: Path) -> None:
        path.write_text(json.dumps(json_value(values), indent=2) + "\n", encoding="utf-8")

    def write_csv(path: Path) -> None:
        columns: List[str] = list(values[0]) if values else []
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(values)

    _replace_file(json_path, write_json)
    _replace_file(csv_path, write_csv)
    return {"json": str(json_path), "csv": str(csv_path)}
