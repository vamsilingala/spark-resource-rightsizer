# Source provenance and contribution policy

## Purpose

This repository is intended to contain an independent implementation of a generic Spark capacity assessment
tool. Its design is based on public product APIs and public Apache Spark event contracts, not on a customer's
private implementation.

## Permitted inputs

Contributions may be based on:

- public Apache Spark documentation and listener-event field names;
- public Databricks SDK and system-table documentation;
- public AWS Glue, CloudWatch, and worker-type documentation;
- original algorithms, tests, examples, and explanatory text written for this project;
- synthetic or explicitly licensed test fixtures.

## Prohibited inputs

Do not contribute employer or customer code, notebooks, internal documentation, data, identifiers, paths,
pricing tables, credentials, screenshots, or configuration. Do not copy code merely by changing names or
formatting. If the origin of a contribution is uncertain, do not add it until ownership is resolved.

## Review checklist

Before accepting a change, verify that:

1. its author can account for the source of the implementation;
2. examples and tests use generated data and fictional identifiers;
3. values that vary by tenant, region, contract, or policy remain configuration inputs;
4. no secrets or personal/customer identifiers are present;
5. any third-party material is compatible with the project license and attributed when required.

This file records engineering controls, not a legal determination of ownership. Obtain qualified legal review
when employment agreements or prior work create uncertainty.
