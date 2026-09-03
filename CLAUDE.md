# insurance-quotes

This file extends [`../CLAUDE.md`](../CLAUDE.md) (shared cross-repo coordination).

## Purpose

Generates insurance quotes for a given policy/carrier. Consumed by the `coveragecompassai` backend during the consumer quote flow.

## Stack

- Python + FastAPI
- Runs on port `8002` locally (`uvicorn src.insurance_quotes.main:app --reload`)
- Swagger UI: `http://127.0.0.1:8002/docs`
- Health: `GET /health` → `{ "status": "ok" }`

## Coupling

- **Consumer**: `coveragecompassai` backend (via the `insurance` router or a dedicated quote client).
- **No direct frontend coupling.** FE never hits this service directly.
- Contract for the consumer lives on the calling side in `coveragecompassai`.

## Conventions specific to this repo

- See README.md for venv setup and Docker build instructions.
- Keep the API surface narrow — this service is a per-carrier quote generator, not a general policy CRUD service.
- New carrier integrations: add as separate modules, expose through a consistent quote-request shape.

## When the backend asks for a new carrier or new quote shape

1. Add the carrier/shape here.
2. Update the consumer client in `coveragecompassai`.
3. Announce to the backend session.

Use the request format from `../CLAUDE.md`.

## CI

`.github/workflows/ci.yml` runs on every PR: `lint`, `build`, `dependency-audit`,
`secret-scan` — same job names and 90-day artifact retention as the sibling repos.

Two things are missing here that the siblings have, both for concrete reasons:

- **No test job.** `app/tests/home/test_home_insurance.py` is a real test with real
  assertions, but importing `app.services.calculations.home.home_insurance` runs
  `_calculator._ensure_loaded()` at module scope, which queries the
  `home_county_factors` collection in MongoDB. Making that module importable without
  a database is the prerequisite for a test job, and is worth doing.
- **No boot check** (`insurance-graph` has one). `app/main.py` calls
  `initialize_vehicle_rates_chromadb()` at module scope, which wants the ~281MB
  `vehicle_rates_chroma_db/` the Dockerfile bakes in and `.gitignore` keeps out,
  falling back to an S3 download. There is nothing importable in a checkout.

`lint` is non-blocking because it is red on `main`. Unlike `cc-ui`'s eslint backlog,
the report is nine lines and every one is a real defect — see the header of
`ruff.toml`. Both were found on the first run:

- `app/services/vector_databases/vehicle_rates_vector.py` **does not parse** (missing
  commas at lines 97-100 and 214). Every importer is currently commented out, so
  nothing breaks in production today. `int(row['grg'], ' ')` would also raise once
  the commas are added — `int()`'s second argument is a base. Its
  `vehicle_rates_vector copy.py` sibling has the same fault and looks like a stray
  backup that should not be tracked.
- `app/services/calculations/home/home_insurance.py:209` uses `coverage_c`, which is
  never a parameter of `HomeInsuranceCalculator.calculate()` nor assigned before use.
  Any quote with an endorsement priced `percent_of_coverage_c`
  (`REPLACEMENT_PERSONAL_PROPERTY`) raises `NameError`. This one is reachable from
  the public entry point.

`build` is blocking and parses every module *except* those two, named explicitly in
the workflow so a syntax error in any third file still fails. Drop the `-x` argument
in the PR that fixes them.

`dependency-audit` must pass `--extra-index-url https://download.pytorch.org/whl/cpu`;
`torch==2.9.1+cpu` is a local-version wheel that exists only on the PyTorch index, and
without the flag pip-audit fails to resolve the file and audits nothing. `torch` is
still reported as skipped, so it is not covered.
