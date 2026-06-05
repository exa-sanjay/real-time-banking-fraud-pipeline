# Contributing

## Scope

This solution is intended to stay self-contained so it can be copied into a larger solutions repository as a single folder.

Keep changes focused on:

- reproducible local demo setup
- clear business-facing documentation
- Exasol-oriented SQL, streaming, and fraud-scoring workflows

## Before Opening a Pull Request

Run these checks from the solution folder:

```bash
docker compose --env-file .env.example config --quiet
python -m compileall -q .
```

Confirm these expectations before pushing:

- `.env` is not tracked
- generated model artifacts are not tracked
- markdown links resolve locally
- demo assets still render in the README

## Change Guidelines

- Prefer explicit version pinning for dependencies and container images.
- Keep host-exposed services bound to `127.0.0.1` unless there is a clear demo requirement to widen access.
- Avoid adding solution logic outside this folder unless the change is repo infrastructure such as CI.
- Document any new environment variables in `.env.example` and `README.md`.
