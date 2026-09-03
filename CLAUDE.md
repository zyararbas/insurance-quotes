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

`lint` and `build` both block. `lint` was non-blocking when CI was introduced,
because `ruff check .` was red on `main`; it passes clean now and a finding in a PR
is a new one. See `ruff.toml` for what is ignored — style codes only, with `E9`
(syntax) and `F821` (undefined name) both live, since those are what caught the
defects below.

Three real defects were found and fixed this way, and the third is the reason the
order matters:

- `app/services/vector_databases/vehicle_rates_vector.py` **did not parse** — four
  dict entries were missing commas, and `int(row['grg'], ' ')` passed a default into
  an argument that takes a base. Its byte-identical `vehicle_rates_vector copy.py`
  duplicate was deleted.
- `app/services/calculations/home/home_insurance.py:209` used `coverage_c`, which is
  never assigned. Any endorsement priced `percent_of_coverage_c` raised `NameError`
  from `calculate_home_insurance()`. **It now raises an explicit `ValueError`
  instead** — see below.
- `vehicle_rates_vector.py:208` used `target_trim_tokens`, never assigned, while
  `target_description_tokens` was assigned and never used. **This was invisible until
  the parse errors were fixed**: ruff cannot resolve names in a module it cannot
  parse, so one bug was hiding another.

**Coverage C is not modelled, and the calculator now says so.** `coverage_package`
returns `coverage_a_dwelling` and nothing else, despite comments that used to claim
B/C/D were "derived by standard ratios" — nothing derived them. Rather than guess a
ratio into a quote, the `percent_of_coverage_c` branch raises with an explanation.
One ratio would not serve all four coverage types the endorsement applies to: for
`HOMEOWNERS`/`MOBILEHOME` Coverage C is a fraction of the dwelling limit, while for
`CONDOMINIUM`/`RENTERS` the personal-property limit *is* the primary limit.
`REPLACEMENT_PERSONAL_PROPERTY` is unsellable until that derivation is defined.

`dependency-audit` must pass `--extra-index-url https://download.pytorch.org/whl/cpu`;
`torch==2.9.1+cpu` is a local-version wheel that exists only on the PyTorch index, and
without the flag pip-audit fails to resolve the file and audits nothing. `torch` is
still reported as skipped, so it is not covered.
