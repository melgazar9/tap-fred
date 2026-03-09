# Repository Guidelines

## Project Structure & Module Organization
- `tap_fred/` contains the extractor implementation:
  - `tap.py` defines `TapFRED` and config schema.
  - `client.py` contains shared API client/retry/throttling logic.
  - `streams/*_streams.py` groups stream classes by FRED domain (series, categories, releases, sources, tags, GeoFRED).
- `tests/` holds automated coverage (`test_core.py`, `test_integration.py`, `test_comprehensive.py`).
- `meltano.yml` is the local runtime/config entrypoint for extractor + target settings.
- `output/` and `state_backups/` are runtime artifacts; avoid committing generated files.

## Build, Test, and Development Commands
- `uv sync` installs project and dev dependencies from `pyproject.toml`/`uv.lock`.
- `uv run tap-fred --help` confirms the CLI entrypoint works.
- `uv run pytest` runs the full Python test suite.
- `uv run pytest tests/test_core.py -k point_in_time` runs focused tests during iteration.
- `tox -e py311` runs tests in a tox-managed interpreter (see `tox.ini` matrix).
- `meltano invoke tap-fred --discover` validates Singer discovery output.
- `meltano el --state-id local-dev tap-fred target-jsonl --select series_observations` runs incremental extraction with explicit state tracking.

## Coding Style & Naming Conventions
- Use Python with 4-space indentation and explicit typing on new/changed code.
- Keep stream logic inside domain modules under `tap_fred/streams/`.
- Naming conventions:
  - `snake_case` for files, functions, variables, and config keys.
  - `PascalCase` for classes.
  - Stream identifiers should mirror API resources (example: `release_series`).
- Run formatting/lint/type checks via pre-commit (`ruff`, `ruff-format`, `mypy`) before opening a PR.

## Testing Guidelines
- Test framework is `pytest` (with Singer SDK testing utilities where applicable).
- Prefer mocked HTTP responses for unit/integration tests; avoid live API calls in default CI-style tests.
- Add regression tests for bug fixes, especially around replication keys/state, ALFRED mode, and wildcard ID handling.
- For stateful behavior, validate with Meltano and a fixed `--state-id`.

## Commit & Pull Request Guidelines
- Follow existing history style: short, imperative commit subjects (for example, `fix search text`, `handle 500 errors`).
- Keep commits focused to one logical change and include matching tests.
- PRs should include:
  - What changed and why.
  - Affected streams/config fields.
  - Test evidence (commands run and results).
  - Example extraction command/output when behavior changes.
