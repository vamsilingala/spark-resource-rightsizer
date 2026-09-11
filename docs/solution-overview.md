# Solution overview

Spark Job Rightsizer answers a narrow question: does recent execution evidence justify a bounded capacity
experiment? It is not an automatic tuning system and does not promise savings.

The workflow inventories workloads, selects the newest eligible successful execution, collects host and Spark
signals, and creates a normalized snapshot. The planner first evaluates evidence quality and risk. It then may
propose one worker-quantity trial plus an independent driver-shape trial, or a worker-shape trial when quantity
stays unchanged.

Executor demand is translated to workers using a configurable executors-per-worker value, platform driver
reserve, and explicit headroom. Shape evaluation converts observed q95 CPU and RAM into required capacity and
considers only caller-approved options with compatible architecture and storage safety.

Default policy values are visible in `settings.Guardrails` and can be overridden in configuration. They are
project defaults, not customer measurements. Prices are always external inputs.

The report includes the existing layout, proposed values, operation names, evidence, explanation, assurance,
and the scope of any cost projection. Missing evidence produces a reasoned no-change result.
