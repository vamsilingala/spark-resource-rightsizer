# Client onboarding

Create one untracked configuration per environment. Use read-only SDK credentials, a client-approved shape
allowlist, and client-supplied regional or contracted prices. Keep generated assessments in storage approved by
the client.

Start with one workload and compare the generated snapshot against the platform UI. Review missing telemetry
before evaluating any recommendation. Test one proposed change at a time, record rollback values, and compare
runtime, SLA, failures, queueing, shuffle, spill, garbage collection, and cost per successful execution.

Never move source code, notebooks, identifiers, data, or credentials between client environments. Improvements
to the generic engine should be independently implemented against public interfaces and validated with
synthetic fixtures.
