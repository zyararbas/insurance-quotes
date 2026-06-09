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
