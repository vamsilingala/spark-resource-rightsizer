"""Evidence-driven, provider-independent capacity planning."""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

from spark_rightsizer.domain import ChangePlan, HostSignals, MachineShape, Snapshot
from spark_rightsizer.settings import Guardrails
from spark_rightsizer.statistics import percentage_change


class CapacityPlanner:
    """Produce one conservative trial plan from a normalized workload snapshot."""

    def __init__(self, guardrails: Optional[Guardrails] = None) -> None:
        self.rules = guardrails or Guardrails()

    def build(
        self,
        snapshot: Snapshot,
        catalog: Dict[str, MachineShape],
        worker_choices: Sequence[str],
        driver_choices: Sequence[str],
    ) -> ChangePlan:
        plan = ChangePlan()
        self._plan_worker_quantity(snapshot, plan)

        count_change = any(
            value is not None for value in (plan.fixed_workers, plan.scale_floor, plan.scale_ceiling)
        )
        worker_shape, worker_note, worker_assurance = self._plan_shape(
            current_key=snapshot.layout.worker_shape,
            signals=snapshot.workers,
            snapshot=snapshot,
            catalog=catalog,
            choices=worker_choices,
            is_worker=True,
        )
        if count_change and worker_shape:
            plan.worker_shape_explanation = (
                "Worker shape evaluation found an option, but the plan changes only worker quantity in this trial."
            )
        else:
            plan.worker_shape = worker_shape
            plan.worker_shape_explanation = worker_note

        driver_assurance = "high"
        if snapshot.layout.driver_shape:
            plan.driver_shape, plan.driver_shape_explanation, driver_assurance = self._plan_shape(
                current_key=snapshot.layout.driver_shape,
                signals=snapshot.driver,
                snapshot=snapshot,
                catalog=catalog,
                choices=driver_choices,
                is_worker=False,
            )
        else:
            plan.driver_shape_explanation = "This runtime does not expose an independently configurable driver."

        self._operations(plan)
        self._cost_projection(snapshot, plan, catalog)
        if plan.operations == ["no_change"]:
            plan.assurance = "high" if snapshot.spark.is_complete else "low"
        elif "low" in {worker_assurance, driver_assurance}:
            plan.assurance = "low"
        elif "medium" in {worker_assurance, driver_assurance}:
            plan.assurance = "medium"
        else:
            plan.assurance = "high"
        return plan

    def _plan_worker_quantity(self, snapshot: Snapshot, plan: ChangePlan) -> None:
        layout = snapshot.layout
        evidence = snapshot.spark
        signals = snapshot.workers
        if not evidence.is_complete or evidence.tasks == 0:
            plan.capacity_explanation = "No quantity change: complete Spark execution evidence is required."
            return
        if not signals.usable or signals.observations < self.rules.minimum_host_observations:
            plan.capacity_explanation = (
                f"No quantity change: at least {self.rules.minimum_host_observations} host observations are required."
            )
            return
        blockers = self._worker_risks(snapshot)
        if blockers:
            plan.capacity_explanation = "No quantity change while these risks are present: " + ", ".join(blockers)
            return
        if evidence.executor_count_q95 is None or evidence.executor_count_max is None:
            plan.capacity_explanation = "No quantity change: executor concurrency was not observed."
            return

        per_worker = max(0.25, float(layout.attributes.get("executors_per_worker", 1.0)))
        driver_reserve = max(0, int(layout.attributes.get("driver_worker_reserve", 0)))
        demand = math.ceil(evidence.executor_count_q95 * self.rules.executor_headroom / per_worker)
        desired = max(layout.platform_minimum, demand + driver_reserve)

        if layout.uses_autoscaling:
            current_floor = layout.scale_floor or layout.platform_minimum
            current_ceiling = layout.scale_ceiling or current_floor
            peak_demand = math.ceil(evidence.executor_count_max * self.rules.executor_headroom / per_worker)
            desired_ceiling = max(current_floor, desired, peak_demand + driver_reserve)
            if desired < current_floor:
                plan.scale_floor = desired
                plan.capacity_explanation = (
                    f"Lower the autoscaling floor from {current_floor} to {desired}; the q95 executor demand "
                    f"includes {self.rules.executor_headroom:.2f}x headroom."
                )
            elif desired_ceiling < current_ceiling:
                plan.scale_ceiling = desired_ceiling
                plan.capacity_explanation = (
                    f"Trial an autoscaling ceiling of {desired_ceiling} instead of {current_ceiling}; "
                    "measure realized cost and queueing before rollout."
                )
            else:
                plan.capacity_explanation = "Observed executor demand fits the current autoscaling range."
            return

        current = layout.fixed_workers
        if current is None or current <= layout.platform_minimum:
            plan.capacity_explanation = "Fixed capacity is already at its configured minimum."
            return
        step_floor = math.ceil(current * (1.0 - self.rules.maximum_step_reduction_percent / 100.0))
        proposed = max(desired, step_floor, layout.platform_minimum)
        if proposed < current:
            plan.fixed_workers = proposed
            plan.capacity_explanation = (
                f"Trial {proposed} workers instead of {current}; q95 executor demand is "
                f"{evidence.executor_count_q95:.1f} with {self.rules.executor_headroom:.2f}x headroom."
            )
        else:
            plan.capacity_explanation = "Observed executor demand does not support a smaller fixed cluster."

    def _worker_risks(self, snapshot: Snapshot) -> List[str]:
        evidence = snapshot.spark
        signals = snapshot.workers
        risks: List[str] = []
        if evidence.failed_tasks:
            risks.append("failed tasks")
        if evidence.hazards:
            risks.append("listener-log failures")
        if evidence.gc_share >= self.rules.stop_gc_share:
            risks.append("high garbage-collection share")
        if evidence.spill_to_input >= self.rules.stop_spill_to_input:
            risks.append("high disk-spill ratio")
        if (
            evidence.stage_skew_q95_median is not None
            and evidence.stage_skew_q95_median >= self.rules.stop_stage_skew
        ):
            risks.append("high within-stage skew")
        if (signals.cpu_q95 or 0.0) >= self.rules.stop_cpu_q95_percent:
            risks.append("high worker CPU q95")
        if (signals.cpu_max or 0.0) >= self.rules.stop_cpu_max_percent:
            risks.append("high worker CPU peak")
        if (signals.ram_q95 or 0.0) >= self.rules.stop_ram_q95_percent:
            risks.append("high worker RAM q95")
        if (signals.ram_max or 0.0) >= self.rules.stop_ram_max_percent:
            risks.append("high worker RAM peak")
        return risks

    def _plan_shape(
        self,
        current_key: str,
        signals: HostSignals,
        snapshot: Snapshot,
        catalog: Dict[str, MachineShape],
        choices: Sequence[str],
        is_worker: bool,
    ) -> Tuple[Optional[str], str, str]:
        role = "worker" if is_worker else "driver"
        current = catalog.get(current_key)
        if current is None:
            return None, f"No {role} shape change: {current_key!r} is absent from the catalog.", "low"
        if not signals.usable or signals.cpu_q95 is None or signals.ram_q95 is None:
            return None, f"No {role} shape change: CPU and RAM observations are required.", "low"
        if signals.observations < self.rules.minimum_host_observations:
            return None, f"No {role} shape change: the observation count is too small.", "low"
        if is_worker and (not snapshot.spark.is_complete or self._worker_risks(snapshot)):
            return None, "No worker shape change while Spark evidence is incomplete or risky.", "high"
        if (signals.cpu_max or 0.0) >= self.rules.stop_cpu_max_percent:
            return None, f"No {role} shape change because peak CPU is too high.", "high"
        if (signals.ram_max or 0.0) >= self.rules.stop_ram_max_percent:
            return None, f"No {role} shape change because peak RAM is too high.", "high"

        needed_cores = current.cores * signals.cpu_q95 / self.rules.desired_cpu_percent
        needed_ram = current.ram_gib * signals.ram_q95 / self.rules.desired_ram_percent
        needed_cores *= self.rules.shape_headroom
        needed_ram *= self.rules.shape_headroom
        rss_bytes = (
            snapshot.spark.executor_rss_high_watermark
            if is_worker
            else snapshot.spark.driver_rss_high_watermark
        )
        if rss_bytes:
            needed_ram = max(needed_ram, rss_bytes / 1024**3 * self.rules.shape_headroom)

        candidates: List[MachineShape] = []
        for key in choices:
            candidate = catalog.get(key)
            if candidate is None or candidate.key == current.key:
                continue
            if current.cpu_arch not in {"", "managed", "unknown"} and candidate.cpu_arch != current.cpu_arch:
                continue
            if candidate.cores >= current.cores and candidate.ram_gib >= current.ram_gib:
                continue
            if candidate.cores < needed_cores or candidate.ram_gib < needed_ram:
                continue
            if current.scratch_gib and candidate.scratch_gib < current.scratch_gib:
                if snapshot.spark.bytes_disk_spilled or (snapshot.workers.io_wait_q95 or 0.0) > 10.0:
                    continue
            if current.price_per_hour is not None and candidate.price_per_hour is not None:
                if candidate.price_per_hour >= current.price_per_hour:
                    continue
            candidates.append(candidate)
        if not candidates:
            return (
                None,
                f"No approved {role} shape meets {needed_cores:.1f} cores and {needed_ram:.1f} GiB RAM.",
                "high",
            )
        candidates.sort(
            key=lambda item: (
                item.price_per_hour if item.price_per_hour is not None else float("inf"),
                item.cores,
                item.ram_gib,
                item.key,
            )
        )
        selected = candidates[0]
        assurance = "medium" if signals.observations < self.rules.minimum_host_observations * 2 else "high"
        explanation = (
            f"Trial {selected.key}: observed q95 implies {needed_cores:.1f} cores and "
            f"{needed_ram:.1f} GiB RAM after policy headroom."
        )
        return selected.key, explanation, assurance

    @staticmethod
    def _operations(plan: ChangePlan) -> None:
        mapping = (
            (plan.fixed_workers, "set_fixed_worker_count"),
            (plan.scale_floor, "set_autoscale_floor"),
            (plan.scale_ceiling, "set_autoscale_ceiling"),
            (plan.worker_shape, "change_worker_shape"),
            (plan.driver_shape, "change_driver_shape"),
        )
        plan.operations.extend(label for value, label in mapping if value is not None)
        if not plan.operations:
            plan.operations.append("no_change")

    @staticmethod
    def _cost_projection(
        snapshot: Snapshot,
        plan: ChangePlan,
        catalog: Dict[str, MachineShape],
    ) -> None:
        current_worker = catalog.get(snapshot.layout.worker_shape)
        proposed_worker = catalog.get(plan.worker_shape or snapshot.layout.worker_shape)
        current_count = snapshot.layout.baseline_workers
        proposed_count = plan.fixed_workers or plan.scale_floor or current_count
        if not current_worker or not proposed_worker or not current_count or not proposed_count:
            plan.saving_scope = "A cost projection requires worker shape and count information."
            return
        if current_worker.price_per_hour is None or proposed_worker.price_per_hour is None:
            plan.saving_scope = "A cost projection requires caller-supplied hourly prices."
            return
        current_cost = current_count * current_worker.price_per_hour
        proposed_cost = proposed_count * proposed_worker.price_per_hour
        current_driver = catalog.get(snapshot.layout.driver_shape or "")
        proposed_driver = catalog.get(plan.driver_shape or snapshot.layout.driver_shape or "")
        if current_driver and current_driver.price_per_hour is not None:
            current_cost += current_driver.price_per_hour
        if proposed_driver and proposed_driver.price_per_hour is not None:
            proposed_cost += proposed_driver.price_per_hour
        plan.projected_saving_percent = percentage_change(current_cost, proposed_cost)
        plan.saving_scope = (
            "Projection uses configured resource-hour prices at the baseline worker count; "
            "platform fees, storage, network, commitments, discounts, and taxes are excluded."
        )
