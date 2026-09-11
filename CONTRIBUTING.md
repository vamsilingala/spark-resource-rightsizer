# Contributing

Thank you for improving Spark Job Rightsizer.

## Development setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e '.[dev]'
```

Run the checks before opening a pull request:

```bash
pytest
ruff check .
python -m compileall -q src tests
```

## Change guidelines

- Keep recommendation logic independent of cloud and platform APIs.
- Keep credentials, client names, telemetry, reports, prices, and private endpoints out of the repository.
- Follow the source-origin requirements in [PROVENANCE.md](PROVENANCE.md). Do not contribute employer or
  customer code, even after renaming or reformatting it.
- Add tests for every behavior change.
- Explain policy-default and recommendation changes in the pull request.
- Use small commits with a clear problem statement.

By contributing, you agree that your contribution is licensed under Apache License 2.0.
