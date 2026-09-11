# Security policy

## Supported versions

Security fixes are applied to the latest released minor version.

## Reporting a vulnerability

Do not open a public issue for suspected vulnerabilities or exposed credentials. Use GitHub's private
security-advisory reporting for this repository.

Include the affected version, reproduction steps, impact, and any suggested mitigation. Do not include
real client credentials, telemetry, job payloads, or workspace identifiers.

## Credential handling

Spark Job Rightsizer uses the standard credential chains of supported SDKs. Credentials must be supplied
at runtime and must never be committed to configuration, reports, examples, or tests.
