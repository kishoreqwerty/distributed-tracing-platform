"""Read-only HTTP query API over ClickHouse — see docs/DECISIONS.md for
why this lives inside the analyzer package rather than as a standalone
service, and docs/ARCHITECTURE.md for how it fits into the rest of the
pipeline. Runs as its own process (`uvicorn analyzer.api.app:app`),
separate from the main analyzer loop (`analyzer.main`), even though both
share this package and its ClickHouse client code — a request-serving
ASGI app and a synchronous polling loop have no business sharing a
process.
"""
