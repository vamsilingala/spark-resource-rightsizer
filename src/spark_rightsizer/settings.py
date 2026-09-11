"""Configuration loading and validation."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Union
from zoneinfo import ZoneInfo

from spark_rightsizer.domain import Cloud, MachineShape, Runtime

_VARIABLE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}")


@dataclass(frozen=True)
class Guardrails:
    minimum_host_observations: int = 12
    desired_cpu_percent: float = 60.0
    desired_ram_percent: float = 65.0
    stop_cpu_q95_percent: float = 72.0
    stop_cpu_max_percent: float = 92.0
    stop_ram_q95_percent: float = 72.0
    stop_ram_max_percent: float = 92.0
    stop_gc_share: float = 0.12
    stop_spill_to_input: float = 0.10
    stop_stage_skew: float = 8.0
    maximum_step_reduction_percent: float = 30.0
    executor_headroom: float = 1.30
    shape_headroom: float = 1.15


@dataclass(frozen=True)
class SourceSettings:
    kind: Runtime
    cloud: Cloud
    region: str
    parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Settings:
    source: SourceSettings
    as_of: date
    timezone: str = "UTC"
    history_days: int = 14
    workload_allowlist: List[str] = field(default_factory=list)
    limit: int = 0
    report_directory: str = "results"
    policy: Guardrails = field(default_factory=Guardrails)
    shapes: Dict[str, MachineShape] = field(default_factory=dict)
    allowed_worker_shapes: List[str] = field(default_factory=list)
    allowed_driver_shapes: List[str] = field(default_factory=list)

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


def _resolve_environment(value: Any) -> Any:
    if isinstance(value, str):
        def substitute(match: re.Match[str]) -> str:
            key, fallback = match.group(1), match.group(2)
            if key in os.environ:
                return os.environ[key]
            if fallback is not None:
                return fallback
            raise ValueError("Missing environment variable: " + key)

        return _VARIABLE.sub(substitute, value)
    if isinstance(value, list):
        return [_resolve_environment(item) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_environment(item) for key, item in value.items()}
    return value


def _document(path: Path) -> Mapping[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        value = json.loads(raw)
    else:
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError("Install PyYAML to read YAML settings") from exc
        value = yaml.safe_load(raw)
    if not isinstance(value, dict):
        raise ValueError("Settings must contain a top-level object")
    return _resolve_environment(value)


def _shape(key: str, raw: Mapping[str, Any]) -> MachineShape:
    return MachineShape(
        key=key,
        cores=float(raw["cores"]),
        ram_gib=float(raw["ram_gib"]),
        scratch_gib=float(raw.get("scratch_gib", 0.0)),
        price_per_hour=(float(raw["price_per_hour"]) if raw.get("price_per_hour") is not None else None),
        cpu_arch=str(raw.get("cpu_arch", "managed")),
        units=float(raw.get("units", 1.0)),
    )


def load_settings(path: Union[str, Path]) -> Settings:
    settings_path = Path(path)
    data = _document(settings_path)
    source_data = data.get("source") or {}
    try:
        source = SourceSettings(
            kind=Runtime(str(source_data["kind"]).lower()),
            cloud=Cloud(str(source_data["cloud"]).lower()),
            region=str(source_data["region"]),
            parameters=dict(source_data.get("parameters") or {}),
        )
    except (KeyError, ValueError) as exc:
        raise ValueError("source.kind, source.cloud, and source.region are required") from exc
    if source.kind == Runtime.GLUE and source.cloud != Cloud.AWS:
        raise ValueError("AWS Glue can only be paired with the aws cloud value")
    snapshot_file = source.parameters.get("snapshot_file")
    if source.kind == Runtime.FILE and snapshot_file:
        snapshot_path = Path(str(snapshot_file)).expanduser()
        if not snapshot_path.is_absolute():
            source.parameters["snapshot_file"] = str(
                (settings_path.resolve().parent / snapshot_path).resolve()
            )

    window = data.get("window") or {}
    timezone_name = str(window.get("timezone", "UTC"))
    zone = ZoneInfo(timezone_name)
    end_value = window.get("end", "today")
    as_of = datetime.now(zone).date() if str(end_value).lower() == "today" else date.fromisoformat(str(end_value))

    raw_policy = dict(data.get("policy") or {})
    unknown = set(raw_policy) - set(Guardrails.__dataclass_fields__)
    if unknown:
        raise ValueError("Unknown policy fields: " + ", ".join(sorted(unknown)))
    policy = Guardrails(**raw_policy)
    if policy.minimum_host_observations < 1:
        raise ValueError("minimum_host_observations must be positive")
    if not 0.0 < policy.executor_headroom:
        raise ValueError("executor_headroom must be positive")

    output = data.get("output") or {}
    settings = Settings(
        source=source,
        as_of=as_of,
        timezone=timezone_name,
        history_days=int(window.get("days", 14)),
        workload_allowlist=[str(item) for item in data.get("workload_allowlist") or []],
        limit=int(data.get("limit", 0)),
        report_directory=str(output.get("directory", "results")),
        policy=policy,
        shapes={str(key): _shape(str(key), value) for key, value in (data.get("shapes") or {}).items()},
        allowed_worker_shapes=[str(item) for item in data.get("allowed_worker_shapes") or []],
        allowed_driver_shapes=[str(item) for item in data.get("allowed_driver_shapes") or []],
    )
    if settings.history_days < 1 or settings.limit < 0:
        raise ValueError("window.days must be positive and limit cannot be negative")
    return settings
