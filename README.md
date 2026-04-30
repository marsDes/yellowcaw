# JD Order Sync and Analytics

This project provides three features:

- Pull order detail data from JD Union API into local SQLite
- Run one-time sync or continuous sync every 5 minutes
- Sync incrementally by `API total - records table count` for the day
- Run basic analytics on stored order data

## 1) Setup

```bash
python -m venv .venv
.venv\Scripts\activate
python main.py -h
```

## 2) Configure Cookie (recommended: env var)

PowerShell:

```powershell
$env:JD_COOKIE = "your full cookie"
```

Optional environment variables:

- `JD_DB_PATH`: SQLite path (default `jd_orders.db`)
- `JD_PLATFORM_LIST`: platform IDs, comma-separated (default `213,390,433`)
- `JD_ACCOUNT_NAME` / `JD_ACCOUNT_ID` / `JD_AD_ID` / `JD_TASK_ID` / `JD_AD_NAME`

## 3) Run one sync

Sync always uses one day (today), with format `YYYY-MM-DD` (e.g. `2026-04-01`).

```bash
python main.py sync
```

Print request payload and response JSON in console:

```bash
python main.py sync --print-response
```

Synced rows are stored in table `records` with business fields directly (no `run_id/page_index/row_index/fetched_at`).

Per sync trigger, snapshot behavior is:

- If `cur` has data, clear `old` first, then move `cur` -> `old`
- Insert newly added rows of this trigger into both `records` and `cur`

Abnormal order marking:

- Rules are stored in table `abnormal_rules` (not hardcoded in sync logic)
- Default seeded rules: specific `sku_id` list with amount range `650 ~ 655`
- On each sync, compare `cur UNION old`; matched orders are marked as `is_abnormal = 1` in `records`
- After each sync that fetched data, abnormal `order_id` values are printed in console

## 4) Sync every 5 minutes

```bash
python main.py daemon --interval 300
```

With request/response output:

```bash
python main.py daemon --interval 300 --print-response
```

`300` seconds means once every 5 minutes.

## 5) Analyze data

Analyze the last 7 days:

```bash
python main.py analyze --days 7
```

Output includes:

- Daily record counts
- Distinct order counts
- Total fee (`pay_amount - refund_amount`)
- Top 10 order statuses

## 6) Abnormal dashboard

Run dashboard page for `records.is_abnormal = 1`:

```bash
python main.py dashboard --host 127.0.0.1 --port 8787 --interval 300 --page-size 100
```

Open in browser:

`http://127.0.0.1:8787`

Dashboard mode also starts background sync every 5 minutes by default.
The page auto-refreshes data every 5 seconds (no manual refresh needed).

If a sync fails with API `code != 1`, the page shows a cookie input box.
Submitting it updates `JD_COOKIE` (env + `.env`) and triggers an immediate sync.
Periodic sync continues even when failures happen.

Displayed fields:

- `order_id`
- `sku_id`
- `platform_name`
- `account_id`
- `account_name`
