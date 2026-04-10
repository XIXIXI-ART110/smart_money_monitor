---
name: smart-money-check
description: Use when working on this smart_money_monitor project to review or validate backend API behavior, /api/run-once fault tolerance, /api/opportunities low-position opportunity pool logic, board-specific opportunity strategies, frontend/backend field compatibility, and edge cases such as None, empty lists, divide-by-zero, partial stock failures, mode/badge/board fields, sorting, and response structures. Do not use for unrelated UI-only styling changes, general finance advice, rewriting StockScoreService from scratch, or tasks outside this project.
---

# Smart Money Check

Use this skill as the unified validation checklist for `smart_money_monitor` changes involving backend API behavior, opportunity pool logic, or frontend/backend data contracts.

## When To Use

Use this skill when the task touches any of:

- `/api/run-once` response shape, fault tolerance, per-stock processing, timeout handling, or `failed_symbols`.
- `/api/opportunities` response shape, `board` filtering, low-position opportunity pool, formal pool, observation/fallback list, sorting, or `mode` / `badge` / `board` fields.
- Frontend rendering that consumes `results`, `failed_symbols`, `items`, `score`, `sub_scores`, `board`, `board_name`, `mode`, or `badge`.
- Runtime safety around `None`, empty lists, missing keys, divide-by-zero, data-source failure, or partially failed stocks.

## When Not To Use

Do not use this skill for:

- Pure visual/style changes that do not touch API fields or opportunity data.
- Rewriting the scoring system or replacing `StockScoreService`.
- General stock-market recommendations or trading advice.
- Unrelated endpoints, unrelated frontend pages, or infrastructure-only tasks.

## Review Workflow

1. Inspect the changed files first with `git diff --stat` and targeted `git diff`.
2. Identify whether the change affects `/api/run-once`, `/api/opportunities`, frontend consumers, or shared scoring inputs.
3. Verify backend syntax and imports before deeper behavior checks.
4. Verify response structures and field compatibility before changing frontend rendering.
5. Prefer small, focused fixes in the batch/service layer. Do not rewrite `StockScoreService` unless explicitly requested.
6. Keep existing `results` and opportunity item structures compatible; only add fields when needed.
7. Finish with a concise report containing exactly these sections: `修改文件`, `问题`, `修复`, `验证方式`.

## Backend Checks

Run these checks when Python files changed:

```bash
venv/bin/python -m py_compile api.py modules/run_service.py modules/opportunity_service.py
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -c 'import api; import modules.run_service; import modules.opportunity_service; print("import ok")'
```

If the local environment writes logs or pycache and sandbox blocks the command, request approval rather than skipping the check.

Check for:

- Syntax errors, missing imports, undefined variables, wrong function signatures.
- Unhandled exceptions in API routes and service orchestration layers.
- `None` handling for optional dict/list fields.
- Divide-by-zero risks when calculating weights, percentages, drawdowns, or averages.
- Empty input handling for watchlists, result lists, opportunity candidates, and fallback pools.

## `/api/run-once` Checklist

Confirm these behaviors:

- One stock failing must not make the entire endpoint fail.
- Per-stock failures should be caught in the batch/future collection layer or equivalent orchestration layer.
- Failed stocks should be skipped from successful `results` and included in stable `failed_symbols`.
- Partial failure response should keep `ok: true`, `message: "run once completed"`, `results: [...]`, and `failed_symbols: [...]`.
- All-stock failure should return JSON with `ok: true`, `results: []`, `failed_symbols: [...]`, and `message: "全部股票获取失败"`.
- Existing result item fields such as `code`, `name`, `status`, `score`, `market_data`, `fund_flow`, `analysis`, and `ai_summary` must remain compatible for successful stocks.

Example expected shape:

```json
{
  "ok": true,
  "message": "run once completed",
  "data": {
    "results": [],
    "failed_symbols": ["301696"]
  }
}
```

## `/api/opportunities` Checklist

Confirm these behaviors:

- `/api/opportunities?board=all|gem|sz_main|sh_main|star` is accepted without changing the path.
- Unsupported `board` values should safely normalize to `all` or otherwise return a stable JSON response.
- Opportunity items should preserve existing fields and add `board`, `board_name`, `mode`, and `badge`.
- `score.sub_scores` remains the source for low/trend/volume/capital; do not rewrite `StockScoreService`.
- Board-specific weighted score should be derived in the opportunity layer, not inside `StockScoreService`.
- Formal pool filtering uses board thresholds.
- If formal pool is empty, observation/fallback list is returned with `mode: "fallback"` or equivalent.
- Sorting follows the board-specific priorities.

Board recognition:

- `300` -> `gem`
- `000`, `001`, `002`, `003` -> `sz_main`
- `600`, `601`, `603`, `605` -> `sh_main`
- `688` -> `star`
- Other prefixes -> `other`

Board strategy checks:

- `gem`: weight low 30%, trend 30%, volume 25%, capital 15%; formal gate `total>=48`, `trend>=10`, `volume>=8`, `low>=10`; sort by `trend`, `volume`, `total`, `low`, `capital`.
- `sz_main`: weight low 35%, trend 25%, volume 20%, capital 20%; formal gate `total>=45`, `low>=12`, `trend>=8`; sort by `low`, `trend`, `total`, `capital`, `volume`.
- `sh_main`: weight low 40%, trend 20%, volume 15%, capital 25%; formal gate `total>=43`, `low>=14`, `capital>=6`; sort by `low`, `capital`, `total`, `trend`, `volume`.
- `star`: weight low 25%, trend 35%, volume 25%, capital 15%; formal gate `total>=50`, `trend>=12`, `volume>=8`; sort by `trend`, `volume`, `total`, `low`, `capital`.

Fallback checks:

- `gem` and `star`: observation list top 5, trend and volume first.
- `sz_main`: top 5 by comprehensive score.
- `sh_main`: top 3 by low position plus capital support.

## Frontend Contract Checks

Check `frontend/app.js` and `frontend/index.html` when opportunity UI changes:

- The frontend calls `/api/opportunities?board=<board>` and does not break other API paths.
- Board buttons map to `all`, `gem`, `sz_main`, `sh_main`, and `star`.
- Cards and details display `board_name` and `badge` only when present.
- Score rendering handles both object score payloads and legacy numeric scores.
- Empty opportunity lists render an empty state instead of throwing.
- Missing `sub_scores`, `metrics`, `features`, `board`, `mode`, or `badge` do not break rendering.

If Node is available, run:

```bash
node --check frontend/app.js
```

If Node is unavailable, state that JS syntax checking could not be run and rely on careful review plus browser/manual verification.

## Suggested Behavior Simulation

Use monkeypatch-style local simulations when real market data would be slow or flaky. Cover at least:

- One successful stock and one failed stock for `/api/run-once`.
- All stocks failed for `/api/run-once`.
- One formal `gem` opportunity.
- One fallback `gem` or `star` opportunity list.
- One fallback `sh_main` list where low + capital sorting wins.
- API wrapper for `/api/opportunities?board=gem`.

Keep simulations local and avoid external network dependency unless the user explicitly wants real data verification.

## Required Final Output

Always include these sections:

**修改文件**
- List each changed file and its purpose.

**问题**
- State the issue or risk found. If no issue was found, say so explicitly and mention residual risks.

**修复**
- Summarize the exact fix without turning it into a long changelog.

**验证方式**
- List commands or manual steps used.
- Include any skipped checks and why, such as `node` unavailable or external data source not tested.
