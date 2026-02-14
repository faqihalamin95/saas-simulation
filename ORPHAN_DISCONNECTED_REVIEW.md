# Orphan & Disconnected Code Review

## Scope
Repository reviewed: `generator/*.py` and `test.py`.

## Findings

### 1) Unused configuration constants (orphan)
In `generator/config.py`, several constants are declared but never consumed by the active pipeline (`generator/runner.py`):
- `END_MONTH`, `MONTH_RANGE`
- `MIN_NEW_USERS`, `MAX_NEW_USERS`
- `CHAOS_EVENTS`
- `BASE_OUTPUT_PATH`, `SUBSCRIPTION_PATH`, `PAYMENTS_PATH`, `PRODUCT_EVENTS_PATH`, `USERS_PATH`
- helper function `get_month_index`

This suggests planning artifacts for a multi-month/incremental design that is not connected to current execution.

### 2) Chaos functions defined but not connected (disconnected)
In `generator/chaos.py`, these helpers exist but are never called by `apply_chaos()` nor the runner:
- `inject_duplicates()`
- `inject_null_fields()`
- constants `DUPLICATE_RATE`, `NULL_FIELD_RATE`

Current `apply_chaos()` only applies late events, time shift, and plan rename.

### 3) Output path configs disconnected from writer
`generator/writer.py` hardcodes `RAW_BASE_PATH = "../raw"`, while `generator/config.py` defines output path constants. These two path systems are disconnected, increasing drift risk.

### 4) Pipeline currently single-month only
`generator/runner.py` loops users but only processes `START_MONTH` once. Meanwhile config contains a full month range (`MONTH_RANGE`) and chaos schedule (`CHAOS_EVENTS`) that imply multi-month simulation.

### 5) Validation script miswired for payments
`test.py` loads only `user_lifecycle` and `product_events`, but payment checks are attempted against `prods` (product events), not a dedicated payments dataframe. This creates disconnected validation logic for payment integrity.

### 6) Minor orphan imports
- `generator/config.py` imports `datetime` but does not use it.

## Recommendations (priority)

1. **Connect monthly loop** in `runner.py` to `MONTH_RANGE` so lifecycle and chaos schedule are actually used.
2. **Unify output paths** by importing path constants from `config.py` into `writer.py` (single source of truth).
3. **Either wire or remove dead chaos helpers** (`inject_duplicates`, `inject_null_fields`) to avoid zombie API surface.
4. **Fix validation input wiring** in `test.py` to load `payments/**/*.parquet` for payment checks.
5. **Remove minor unused imports/constants** once architecture direction is confirmed.
