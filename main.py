from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:

    def load_dotenv() -> None:
        return None


load_dotenv()
API_URL = (
    "https://jzt-api.jd.com/union/cid/data/queryCallbackOrderDetailList?requestFrom=0"
)

ORDER_COLUMNS = [
    "order_id",
    "sku_id",
    "product_url",
    "sku_name",
    "order_status",
    "pay_amount",
    "refund_amount",
    "click_time",
    "order_time",
    "attribution_time",
    "platform",
    "platform_name",
    "account_id",
    "account_name",
    "ad_id",
    "ad_name",
    "track_point",
    "task_id",
    "transmit_back_deduction_reason",
    "executor_pin_type",
]

DEFAULT_ABNORMAL_RULE_SKUS = [
    100166052103,
    100222807550,
    100242784708,
    100249258802,
    100193241893,
    100161114196,
    100184494851,
    100217193577,
    100293350630,
    100300073380,
    100272747214,
]
DEFAULT_ABNORMAL_MIN_PAY_AMOUNT = 650.0
DEFAULT_ABNORMAL_MAX_PAY_AMOUNT = 655.0


@dataclass
class Config:
    cookie: str
    db_path: str
    platform_list: list[int]
    account_name: str
    account_id: str
    ad_id: str
    task_id: str
    ad_name: str
    timeout: int


class ApiCodeError(RuntimeError):
    def __init__(self, code: Any, msg: Any) -> None:
        super().__init__(f"API returned code={code}, msg={msg}")
        self.code = code
        self.msg = msg


def parse_platform_list(raw: str) -> list[int]:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    values = [int(part) for part in parts]
    if not values:
        raise ValueError("platformList must not be empty")
    return values


def load_config(args: argparse.Namespace) -> Config:
    cookie = args.cookie or os.getenv("JD_COOKIE", "").strip()
    if not cookie:
        raise SystemExit("Missing Cookie. Use --cookie or env var JD_COOKIE.")

    db_path = args.db or os.getenv("JD_DB_PATH", "jd_orders.db")
    platform_raw = args.platforms or os.getenv("JD_PLATFORM_LIST", "213,390,433")

    return Config(
        cookie=cookie,
        db_path=db_path,
        platform_list=parse_platform_list(platform_raw),
        account_name=os.getenv("JD_ACCOUNT_NAME", ""),
        account_id=os.getenv("JD_ACCOUNT_ID", ""),
        ad_id=os.getenv("JD_AD_ID", ""),
        task_id=os.getenv("JD_TASK_ID", ""),
        ad_name=os.getenv("JD_AD_NAME", ""),
        timeout=int(os.getenv("JD_HTTP_TIMEOUT", "30")),
    )


def init_db(conn: sqlite3.Connection) -> None:
    ensure_records_table(conn)
    ensure_snapshot_tables(conn)
    ensure_abnormal_rules_table(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_order_id ON records(order_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_order_status ON records(order_status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_order_time ON records(order_time)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_is_abnormal ON records(is_abnormal)"
    )
    conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not table_exists(conn, table_name):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def create_order_table(
    conn: sqlite3.Connection, table_name: str, with_abnormal_flag: bool = False
) -> None:
    abnormal_col = (
        "is_abnormal INTEGER NOT NULL DEFAULT 0," if with_abnormal_flag else ""
    )
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            sku_id INTEGER,
            product_url TEXT,
            sku_name TEXT,
            order_status INTEGER,
            pay_amount REAL,
            refund_amount REAL,
            click_time TEXT,
            order_time TEXT,
            attribution_time TEXT,
            platform INTEGER,
            platform_name TEXT,
            account_id TEXT,
            account_name TEXT,
            ad_id TEXT,
            ad_name TEXT,
            track_point INTEGER,
            task_id INTEGER,
            transmit_back_deduction_reason INTEGER,
            executor_pin_type INTEGER,
            {abnormal_col}
            UNIQUE(order_id)
        )
        """)


def create_records_table(conn: sqlite3.Connection) -> None:
    create_order_table(conn, "records", with_abnormal_flag=True)


def ensure_snapshot_tables(conn: sqlite3.Connection) -> None:
    create_order_table(conn, "cur")
    create_order_table(conn, '"old"')


def ensure_abnormal_rules_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS abnormal_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_id INTEGER NOT NULL,
            min_pay_amount REAL NOT NULL,
            max_pay_amount REAL NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            UNIQUE(sku_id, min_pay_amount, max_pay_amount)
        )
        """)

    exists = conn.execute("SELECT COUNT(*) FROM abnormal_rules").fetchone()
    if exists and int(exists[0]) > 0:
        return

    for sku_id in DEFAULT_ABNORMAL_RULE_SKUS:
        conn.execute(
            """
            INSERT INTO abnormal_rules (sku_id, min_pay_amount, max_pay_amount, enabled)
            VALUES (?, ?, ?, 1)
            """,
            (
                sku_id,
                DEFAULT_ABNORMAL_MIN_PAY_AMOUNT,
                DEFAULT_ABNORMAL_MAX_PAY_AMOUNT,
            ),
        )


def rotate_cur_to_old(conn: sqlite3.Connection) -> None:
    cur_count_row = conn.execute("SELECT COUNT(*) FROM cur").fetchone()
    cur_count = int(cur_count_row[0]) if cur_count_row else 0
    if cur_count <= 0:
        return

    cols_sql = ", ".join(ORDER_COLUMNS)
    conn.execute('DELETE FROM "old"')
    conn.execute(f'INSERT INTO "old" ({cols_sql}) SELECT {cols_sql} FROM cur')
    conn.execute("DELETE FROM cur")


def query_abnormal_order_ids(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute("""
        WITH merged AS (
            SELECT order_id, sku_id, pay_amount, account_id FROM cur
            UNION
            SELECT order_id, sku_id, pay_amount, account_id FROM "old"
        ),
        matched AS (
            SELECT
                m.order_id,
                m.account_id,
                r.sku_id,
                r.min_pay_amount,
                r.max_pay_amount
            FROM merged m
           JOIN abnormal_rules r
             ON m.sku_id = r.sku_id
            AND COALESCE(r.enabled, 0) = 1
          WHERE m.order_id IS NOT NULL
            AND m.account_id IS NOT NULL
            AND m.pay_amount >= r.min_pay_amount
        ),
        qualified_rules AS (
            SELECT account_id, sku_id, min_pay_amount, max_pay_amount
            FROM matched
            GROUP BY account_id, sku_id, min_pay_amount, max_pay_amount
            HAVING COUNT(DISTINCT order_id) >= 2
        )
        SELECT DISTINCT m.order_id
        FROM matched m
        JOIN qualified_rules q
          ON m.account_id = q.account_id
         AND m.sku_id = q.sku_id
         AND m.min_pay_amount = q.min_pay_amount
         AND m.max_pay_amount = q.max_pay_amount
        ORDER BY m.order_id
        """).fetchall()
    return [int(row[0]) for row in rows]


def mark_abnormal_orders(conn: sqlite3.Connection) -> tuple[int, list[int]]:
    abnormal_order_ids = query_abnormal_order_ids(conn)
    if not abnormal_order_ids:
        return 0, []

    before = conn.total_changes
    conn.execute("""
        UPDATE records
        SET is_abnormal = 1
        WHERE COALESCE(is_abnormal, 0) = 0
          AND order_id IN (
              WITH merged AS (
                  SELECT order_id, sku_id, pay_amount, account_id FROM cur
                  UNION
                  SELECT order_id, sku_id, pay_amount, account_id FROM "old"
              ),
              matched AS (
                  SELECT
                      m.order_id,
                      m.account_id,
                      r.sku_id,
                      r.min_pay_amount,
                      r.max_pay_amount
                  FROM merged m
                  JOIN abnormal_rules r
                    ON m.sku_id = r.sku_id
                   AND COALESCE(r.enabled, 0) = 1
                  WHERE m.order_id IS NOT NULL
                    AND m.account_id IS NOT NULL
                    AND m.pay_amount >= r.min_pay_amount
              ),
              qualified_rules AS (
                  SELECT account_id, sku_id, min_pay_amount, max_pay_amount
                  FROM matched
                  GROUP BY account_id, sku_id, min_pay_amount, max_pay_amount
                  HAVING COUNT(DISTINCT order_id) >= 2
              )
              SELECT DISTINCT m.order_id
              FROM matched m
              JOIN qualified_rules q
                ON m.account_id = q.account_id
               AND m.sku_id = q.sku_id
                AND m.min_pay_amount = q.min_pay_amount
                AND m.max_pay_amount = q.max_pay_amount
          )
        """)
    return conn.total_changes - before, abnormal_order_ids


def migrate_table_data(conn: sqlite3.Connection, source_table: str) -> int:
    src_columns = table_columns(conn, source_table)
    common_columns = [col for col in ORDER_COLUMNS if col in src_columns]
    if not common_columns:
        return 0

    cols_sql = ", ".join(common_columns)
    before = conn.total_changes
    conn.execute(
        f"INSERT OR IGNORE INTO records ({cols_sql}) SELECT {cols_sql} FROM {source_table}"
    )
    return conn.total_changes - before


def ensure_records_table(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "records"):
        create_records_table(conn)
        migrated = 0
        if table_exists(conn, "order_records"):
            migrated = migrate_table_data(conn, "order_records")
        conn.commit()
        if migrated > 0:
            print(f"[db] migrated {migrated} rows from order_records to records")
        return

    cols = table_columns(conn, "records")
    metadata_columns = {
        "run_id",
        "page_index",
        "row_index",
        "fetched_at",
        "record_json",
    }
    if cols.intersection(metadata_columns):
        legacy_table = f"records_legacy_{int(time.time())}"
        conn.execute(f"ALTER TABLE records RENAME TO {legacy_table}")
        create_records_table(conn)
        migrated = migrate_table_data(conn, legacy_table)
        conn.commit()
        print(f"[db] migrated {migrated} rows from {legacy_table} to records")
        return

    if "is_abnormal" not in cols:
        conn.execute(
            "ALTER TABLE records ADD COLUMN is_abnormal INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()


def build_payload(
    config: Config, start_date: str, end_date: str, page_index: int, page_size: int
) -> dict[str, Any]:
    return {
        "startDate": start_date,
        "endDate": end_date,
        "platformList": config.platform_list,
        "accountName": config.account_name,
        "accountId": config.account_id,
        "adId": config.ad_id,
        "taskId": config.task_id,
        "transmitBackDeductionReason": None,
        "executorPinType": None,
        "trackPoint": None,
        "orderStatus": None,
        "adName": config.ad_name,
        "pageIndex": page_index,
        "pageSize": page_size,
    }


def request_page(config: Config, payload: dict[str, Any]) -> dict[str, Any]:
    raw_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        API_URL,
        data=raw_body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Cookie": config.cookie,
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=config.timeout) as response:
            raw_response = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {body[:300]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc

    try:
        parsed = json.loads(raw_response)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Response is not valid JSON: {raw_response[:300]}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("Response JSON root is not an object")

    code = parsed.get("code")
    if code is not None and code != 1:
        raise ApiCodeError(code=code, msg=parsed.get("msg"))

    return parsed


def check_cookie_status(cookie: str) -> tuple[bool, str]:
    cookie_text = cookie.strip()
    if not cookie_text:
        return False, "JD_COOKIE is empty"

    try:
        platform_list = parse_platform_list(
            os.getenv("JD_PLATFORM_LIST", "213,390,433")
        )
    except Exception:
        platform_list = [213, 390, 433]

    config = Config(
        cookie=cookie_text,
        db_path="",
        platform_list=platform_list,
        account_name=os.getenv("JD_ACCOUNT_NAME", ""),
        account_id=os.getenv("JD_ACCOUNT_ID", ""),
        ad_id=os.getenv("JD_AD_ID", ""),
        task_id=os.getenv("JD_TASK_ID", ""),
        ad_name=os.getenv("JD_AD_NAME", ""),
        timeout=int(os.getenv("JD_HTTP_TIMEOUT", "20")),
    )

    today = dt.datetime.now().strftime("%Y-%m-%d")
    payload = build_payload(config, today, today, 1, 1)
    response = request_page(config, payload)
    code = response.get("code")
    if code == 1:
        return True, "Cookie valid"
    return False, f"code={code}, msg={response.get('msg')}"


def save_cookie_to_env(cookie: str) -> None:
    value = cookie.strip()
    os.environ["JD_COOKIE"] = value

    env_path = os.path.join(os.getcwd(), ".env")
    lines: list[str] = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as file:
            lines = file.read().splitlines()

    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    new_line = f'JD_COOKIE="{escaped}"'

    updated = False
    for index, line in enumerate(lines):
        if line.startswith("JD_COOKIE="):
            lines[index] = new_line
            updated = True
            break

    if not updated:
        lines.append(new_line)

    with open(env_path, "w", encoding="utf-8") as file:
        file.write("\n".join(lines).rstrip() + "\n")


def traverse_path(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidate_paths: list[tuple[str, ...]] = [
        ("data", "result", "list"),
        ("data", "list"),
        ("data", "orderDetailList"),
        ("data", "orderList"),
        ("data", "rows"),
        ("data", "items"),
        ("data", "data"),
        ("result", "list"),
        ("result", "data"),
        ("data", "records"),
        ("records",),
        ("list",),
        ("data",),
    ]
    for path in candidate_paths:
        value = traverse_path(payload, path)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    stack: list[Any] = [payload]
    best: list[dict[str, Any]] = []
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for child in current.values():
                stack.append(child)
            continue
        if isinstance(current, list):
            dict_items = [item for item in current if isinstance(item, dict)]
            if len(dict_items) > len(best):
                best = dict_items
            for item in current:
                stack.append(item)

    return best


def extract_total_pages(payload: dict[str, Any]) -> int | None:
    candidate_paths: list[tuple[str, ...]] = [
        ("data", "result", "totalPage"),
        ("data", "totalPage"),
        ("result", "totalPage"),
        ("data", "result", "pageCount"),
        ("data", "pageCount"),
        ("result", "pageCount"),
    ]
    for path in candidate_paths:
        value = traverse_path(payload, path)
        if value is None:
            continue
        try:
            pages = int(value)
        except (TypeError, ValueError):
            continue
        if pages > 0:
            return pages
    return None


def extract_total_count(payload: dict[str, Any]) -> int | None:
    candidate_paths: list[tuple[str, ...]] = [
        ("data", "total"),
        ("data", "result", "total"),
        ("result", "total"),
        ("total",),
    ]
    for path in candidate_paths:
        value = traverse_path(payload, path)
        if value is None:
            continue
        try:
            total = int(value)
        except (TypeError, ValueError):
            continue
        if total >= 0:
            return total
    return None


def count_existing_records(
    conn: sqlite3.Connection, start_date: str, end_date: str
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM records
        WHERE substr(order_time, 1, 10) >= ? AND substr(order_time, 1, 10) <= ?
        """,
        (start_date, end_date),
    ).fetchone()
    return int(row[0]) if row else 0


def pick_first_text(record: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def pick_first_float(record: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = record.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def pick_first_int(record: dict[str, Any], keys: list[str]) -> int | None:
    for key in keys:
        value = record.get(key)
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def extract_order_values(record: dict[str, Any]) -> tuple[Any, ...]:
    order_id = pick_first_int(record, ["orderId", "unionOrderId", "parentId", "id"])
    sku_id = pick_first_int(record, ["skuId"])
    product_url = pick_first_text(record, ["productUrl"])
    sku_name = pick_first_text(record, ["skuName"])
    order_status = pick_first_int(record, ["orderStatus", "validCode", "status"])
    pay_amount = pick_first_float(
        record, ["payAmount", "actualFee", "actualCommission"]
    )
    refund_amount = pick_first_float(record, ["refundAmount"])
    click_time = pick_first_text(record, ["clickTime"])
    order_time = pick_first_text(
        record, ["orderTime", "finishTime", "payTime", "created"]
    )
    attribution_time = pick_first_text(record, ["attributionTime"])
    platform = pick_first_int(record, ["platform"])
    platform_name = pick_first_text(record, ["platformName"])
    account_id = pick_first_text(record, ["accountId"])
    account_name = pick_first_text(record, ["accountName"])
    ad_id = pick_first_text(record, ["adId"])
    ad_name = pick_first_text(record, ["adName"])
    track_point = pick_first_int(record, ["trackPoint"])
    task_id = pick_first_int(record, ["taskId"])
    transmit_back_deduction_reason = pick_first_int(
        record, ["transmitBackDeductionReason"]
    )
    executor_pin_type = pick_first_int(record, ["executorPinType"])

    return (
        order_id,
        sku_id,
        product_url,
        sku_name,
        order_status,
        pay_amount,
        refund_amount,
        click_time,
        order_time,
        attribution_time,
        platform,
        platform_name,
        account_id,
        account_name,
        ad_id,
        ad_name,
        track_point,
        task_id,
        transmit_back_deduction_reason,
        executor_pin_type,
    )


def insert_into_table(
    conn: sqlite3.Connection, table_name: str, values: tuple[Any, ...]
) -> bool:
    cols_sql = ", ".join(ORDER_COLUMNS)
    placeholders = ", ".join(["?"] * len(ORDER_COLUMNS))
    cursor = conn.execute(
        f"INSERT OR IGNORE INTO {table_name} ({cols_sql}) VALUES ({placeholders})",
        values,
    )
    return cursor.rowcount > 0


def insert_record(conn: sqlite3.Connection, record: dict[str, Any]) -> bool:
    values = extract_order_values(record)
    inserted = insert_into_table(conn, "records", values)
    if inserted:
        insert_into_table(conn, "cur", values)
    return inserted


def sync_once(
    config: Config,
    start_date: str,
    end_date: str,
    page_size: int,
    print_response: bool = False,
) -> tuple[int, int]:
    conn = sqlite3.connect(config.db_path)
    try:
        init_db(conn)
        rotate_cur_to_old(conn)
        conn.commit()

        if print_response:
            print("[snapshot] rotated cur -> old (if cur had data)")

        payload = build_payload(config, start_date, end_date, 1, page_size)
        first_response = request_page(config, payload)

        if print_response:
            print(
                f"[request] pageIndex=1, pageSize={page_size}, startDate={start_date}, endDate={end_date}"
            )
            print("[response] " + json.dumps(first_response, ensure_ascii=False))

        api_total = extract_total_count(first_response)
        existing_total = count_existing_records(conn, start_date, end_date)
        if api_total is None:
            api_total = existing_total

        delta_total = max(api_total - existing_total, 0)
        if print_response:
            print(
                f"[delta] api_total={api_total}, records_total={existing_total}, need_fetch={delta_total}"
            )

        if delta_total <= 0:
            return 0, 0

        max_pages = max(math.ceil(delta_total / page_size), 1)
        page_index = 1
        page_count = 0
        inserted_total = 0
        processed_total = 0

        while page_index <= max_pages and processed_total < delta_total:
            if page_index == 1:
                response = first_response
            else:
                payload = build_payload(
                    config, start_date, end_date, page_index, page_size
                )
                response = request_page(config, payload)
                if print_response:
                    print(
                        f"[request] pageIndex={page_index}, pageSize={page_size}, startDate={start_date}, endDate={end_date}"
                    )
                    print("[response] " + json.dumps(response, ensure_ascii=False))

            records = extract_records(response)
            page_count += 1
            inserted_page = 0

            for record in records:
                if insert_record(conn, record):
                    inserted_total += 1
                    inserted_page += 1

            conn.commit()
            processed_total += len(records)

            if print_response:
                print(
                    f"[parsed] pageIndex={page_index}, records={len(records)}, inserted={inserted_page}, processed_total={processed_total}/{delta_total}"
                )

            if not records or len(records) < page_size:
                break
            page_index += 1

        abnormal_marked, abnormal_order_ids = mark_abnormal_orders(conn)
        conn.commit()
        if abnormal_order_ids:
            ids_text = ",".join(str(order_id) for order_id in abnormal_order_ids)
            print(f"Abnormal orderIds: {ids_text}")
        else:
            print("Abnormal orderIds: none")
        if print_response:
            print(
                f"[abnormal] matched={len(abnormal_order_ids)}, newly_marked={abnormal_marked}"
            )

        return page_count, inserted_total
    finally:
        conn.close()


def analyze_data(db_path: str, days: int) -> None:
    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT
                substr(order_time, 1, 10) AS day,
                COUNT(*) AS total_records,
                COUNT(DISTINCT order_id) AS unique_orders,
                ROUND(SUM(COALESCE(pay_amount, 0) - COALESCE(refund_amount, 0)), 2) AS total_fee,
                SUM(CASE WHEN COALESCE(is_abnormal, 0) = 1 THEN 1 ELSE 0 END) AS abnormal_count
            FROM records
            WHERE datetime(order_time) >= datetime('now', ?)
            GROUP BY day
            ORDER BY day DESC
            """,
            (f"-{days} day",),
        ).fetchall()

        status_rows = conn.execute(
            """
            SELECT COALESCE(CAST(order_status AS TEXT), 'UNKNOWN') AS status, COUNT(*) AS cnt
            FROM records
            WHERE datetime(order_time) >= datetime('now', ?)
            GROUP BY status
            ORDER BY cnt DESC
            LIMIT 10
            """,
            (f"-{days} day",),
        ).fetchall()
    finally:
        conn.close()

    print(f"Analysis for last {days} days")
    if not rows:
        print("No data available")
        return

    print("Daily summary:")
    for day, total_records, unique_orders, total_fee, abnormal_count in rows:
        print(
            f"  {day} | records: {total_records} | unique_orders: {unique_orders} | fee_total: {total_fee} | abnormal: {abnormal_count}"
        )

    if status_rows:
        print("Top status distribution:")
        for status, cnt in status_rows:
            print(f"  {status}: {cnt}")


def run_dashboard(
    db_path: str, host: str, port: int, interval_seconds: int, page_size: int
) -> None:
    state_lock = threading.Lock()
    stop_event = threading.Event()
    state: dict[str, Any] = {
        "last_sync_at": None,
        "last_sync_ts": None,
        "last_result": "idle",
        "last_message": "waiting for first sync",
        "needs_cookie": False,
        "running": False,
    }

    def get_abnormal_rows() -> list[dict[str, Any]]:
        conn = sqlite3.connect(db_path)
        try:
            init_db(conn)
            rows = conn.execute("""
                SELECT order_id, sku_id, order_time, platform_name, account_id, account_name
                FROM records
                WHERE COALESCE(is_abnormal, 0) = 1
                ORDER BY id DESC
                LIMIT 1000
                """).fetchall()
            return [
                {
                    "order_id": row[0],
                    "sku_id": row[1],
                    "order_time": row[2],
                    "platform_name": row[3],
                    "account_id": row[4],
                    "account_name": row[5],
                }
                for row in rows
            ]
        finally:
            conn.close()

    def update_state(**updates: Any) -> None:
        with state_lock:
            state.update(updates)

    def run_one_sync() -> None:
        with state_lock:
            if state.get("running"):
                return
            state["running"] = True

        try:
            config = load_config(
                argparse.Namespace(db=db_path, cookie=None, platforms=None)
            )
            date_text = dt.datetime.now().strftime("%Y-%m-%d")
            pages, inserted = sync_once(
                config,
                date_text,
                date_text,
                page_size,
                print_response=False,
            )
            now_ts = int(time.time())
            update_state(
                last_sync_at=dt.datetime.now().isoformat(timespec="seconds"),
                last_sync_ts=now_ts,
                last_result="ok",
                last_message=f"sync success: pages={pages}, new_records={inserted}",
                needs_cookie=False,
            )
        except SystemExit as exc:
            now_ts = int(time.time())
            update_state(
                last_sync_at=dt.datetime.now().isoformat(timespec="seconds"),
                last_sync_ts=now_ts,
                last_result="failed",
                last_message=f"config error: {exc}",
                needs_cookie=True,
            )
        except ApiCodeError as exc:
            now_ts = int(time.time())
            update_state(
                last_sync_at=dt.datetime.now().isoformat(timespec="seconds"),
                last_sync_ts=now_ts,
                last_result="failed",
                last_message=f"api error: code={exc.code}, msg={exc.msg}",
                needs_cookie=True,
            )
        except Exception as exc:
            now_ts = int(time.time())
            update_state(
                last_sync_at=dt.datetime.now().isoformat(timespec="seconds"),
                last_sync_ts=now_ts,
                last_result="failed",
                last_message=f"sync failed: {exc}",
            )
        finally:
            update_state(running=False)

    def sync_loop() -> None:
        while not stop_event.is_set():
            run_one_sync()
            stop_event.wait(interval_seconds)

    sync_thread = threading.Thread(target=sync_loop, daemon=True)
    sync_thread.start()

    class DashboardHandler(BaseHTTPRequestHandler):
        def _json_response(self, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _html_response(self, content: str) -> None:
            body = content.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            if self.path.split("?", 1)[0] != "/api/update-cookie":
                self.send_error(404, "Not Found")
                return

            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length).decode("utf-8", errors="ignore")
            parsed = urllib.parse.parse_qs(raw_body)
            cookie = parsed.get("cookie", [""])[0].strip()
            if cookie:
                save_cookie_to_env(cookie)
                update_state(
                    needs_cookie=False, last_message="cookie updated, syncing..."
                )
                threading.Thread(target=run_one_sync, daemon=True).start()

            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/api/state":
                with state_lock:
                    current_state = dict(state)
                payload = {
                    "state": current_state,
                    "rows": get_abnormal_rows(),
                    "interval_seconds": interval_seconds,
                }
                self._json_response(payload)
                return

            if path != "/":
                self.send_error(404, "Not Found")
                return

            content = """<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>黄牛单-看板</title>
  <style>
    :root { --bg:#f6f8fb; --card:#fff; --text:#18222d; --muted:#607080; --line:#d9e0e7; --accent:#0a7a6a; }
    * { box-sizing: border-box; }
    body { margin:0; font-family:"Segoe UI","PingFang SC",sans-serif; background:linear-gradient(180deg,#f9fcff 0%,var(--bg) 100%); color:var(--text); }
    .wrap { max-width:1200px; margin:0 auto; padding:24px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:14px; overflow:hidden; box-shadow:0 10px 30px rgba(20,40,60,.06); }
    .head { padding:16px 18px; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; }
    .title { font-size:18px; font-weight:700; }
    .meta { color:var(--muted); font-size:13px; }
    .countdown { font-size:13px; color:#0f5b4f; background:#e8f7f2; border:1px solid #c9e8de; border-radius:999px; padding:6px 10px; white-space:nowrap; }
    .status { margin:12px 18px; padding:10px 12px; border-radius:10px; font-size:13px; border:1px solid var(--line); }
    .status.ok { background:#e8f7f2; color:#0f5b4f; border-color:#c9e8de; }
    .status.bad { background:#fff2f2; color:#8c2d2d; border-color:#f2c4c4; }
    .cookie-form { margin:0 18px 16px; display:none; gap:8px; }
    .cookie-form.show { display:grid; }
    .cookie-form label { font-size:13px; color:var(--muted); }
    .cookie-form textarea { width:100%; min-height:80px; border:1px solid var(--line); border-radius:8px; padding:10px; font-family:"Cascadia Code",Consolas,monospace; font-size:12px; }
    .cookie-form button { width:160px; border:1px solid var(--accent); color:#fff; background:var(--accent); border-radius:8px; padding:8px 12px; cursor:pointer; }
    .table-wrap { overflow:auto; }
    table { width:100%; border-collapse:collapse; min-width:820px; }
    th,td { padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; }
    th { font-size:13px; color:var(--muted); background:#f2f6fa; }
    td { font-size:14px; }
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='card'>
      <div class='head'>
        <div>
          <div class='title'>黄牛单</div>
          <div id='meta' class='meta'>loading...</div>
        </div>
        <div id='countdown' class='countdown'>下次同步倒计时: --s</div>
      </div>
      <div id='status' class='status ok'>loading...</div>
      <form id='cookie-form' class='cookie-form' method='post' action='/api/update-cookie'>
        <label for='cookie'>JD_COOKIE (接口返回 code != 1 时请更新)</label>
        <textarea id='cookie' name='cookie' placeholder='Paste new JD_COOKIE here' required></textarea>
        <button type='submit'>更新 JD_COOKIE 并立即同步</button>
      </form>
      <div class='table-wrap'>
        <table>
          <thead>
            <tr>
              <th>订单Id</th>
              <th>skuId</th>
              <th>下单时间</th>
              <th>平台</th>
              <th>账号Id</th>
              <th>账号名称</th>
            </tr>
          </thead>
          <tbody id='tbody'><tr><td colspan='6'>Loading...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>
  <script>
    window.__nextSyncIn = null;

    async function submitCookieForm(event) {
      event.preventDefault();
      const form = event.currentTarget;
      const formData = new FormData(form);
      try {
        await fetch(form.action, {
          method: 'POST',
          body: new URLSearchParams(formData),
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
        });
        window.location.reload();
      } catch (e) {
        const statusEl = document.getElementById('status');
        statusEl.className = 'status bad';
        statusEl.textContent = 'cookie 更新失败，请重试';
      }
    }

    function updateCountdown() {
      const el = document.getElementById('countdown');
      if (!el) return;
      if (window.__nextSyncIn === null) {
        el.textContent = '下次同步倒计时: --s';
        return;
      }
      const value = Math.max(0, Math.floor(window.__nextSyncIn));
      el.textContent = `下次同步倒计时: ${value}s`;
      if (window.__nextSyncIn > 0) {
        window.__nextSyncIn -= 1;
      }
    }

    async function refresh() {
      try {
        const resp = await fetch('/api/state', { cache: 'no-store' });
        const data = await resp.json();
        const s = data.state || {};
        const rows = data.rows || [];
        const intervalSeconds = Number(data.interval_seconds || 300);

        document.getElementById('meta').textContent = `异常订单 | 总数: ${rows.length} | 间隔: ${intervalSeconds}s`;

        let nextSyncIn = intervalSeconds;
        if (s.running) {
          nextSyncIn = 0;
        } else if (s.last_sync_ts) {
          const tsSeconds = Number(s.last_sync_ts);
          if (!Number.isNaN(tsSeconds) && tsSeconds > 0) {
            const elapsed = Math.floor(Date.now() / 1000 - tsSeconds);
            nextSyncIn = Math.max(intervalSeconds - elapsed, 0);
          }
        }
        window.__nextSyncIn = nextSyncIn;
        updateCountdown();

        const statusEl = document.getElementById('status');
        const isOk = s.last_result === 'ok';
        statusEl.className = 'status ' + (isOk ? 'ok' : 'bad');
        const lastText = s.last_sync_ts ? new Date(Number(s.last_sync_ts) * 1000).toLocaleString() : '-';
        statusEl.textContent = `[${lastText}] ${s.last_message || '-'}`;

        const form = document.getElementById('cookie-form');
        form.className = 'cookie-form' + (s.needs_cookie ? ' show' : '');

        const tbody = document.getElementById('tbody');
        if (!rows.length) {
          tbody.innerHTML = "<tr><td colspan='6'>No abnormal records</td></tr>";
        } else {
          tbody.innerHTML = rows.map(r => `<tr><td>${r.order_id ?? ''}</td><td>${r.sku_id ?? ''}</td><td>${r.order_time ?? ''}</td><td>${r.platform_name ?? ''}</td><td>${r.account_id ?? ''}</td><td>${r.account_name ?? ''}</td></tr>`).join('');
        }
      } catch (e) {
        const statusEl = document.getElementById('status');
        statusEl.className = 'status bad';
        statusEl.textContent = 'dashboard refresh failed';
      }
    }

    refresh();
    document.getElementById('cookie-form').addEventListener('submit', submitCookieForm);
    setInterval(updateCountdown, 1000);
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""
            self._html_response(content)

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard running at http://{host}:{port}")
    print(f"Background sync started: every {interval_seconds}s, page_size={page_size}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        server.server_close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="JD order sync and analytics")
    parser.add_argument("--db", help="SQLite database path, default jd_orders.db")
    parser.add_argument("--cookie", help="JD Cookie, recommend using env var JD_COOKIE")
    parser.add_argument("--platforms", help="Platform IDs separated by commas")

    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="Run one sync (today only)")
    sync_parser.add_argument("--page-size", type=int, default=100, help="Rows per page")
    sync_parser.add_argument(
        "--print-response",
        action="store_true",
        help="Print request payload and response JSON",
    )

    daemon_parser = subparsers.add_parser("daemon", help="Run sync in a polling loop")
    daemon_parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Polling interval seconds, default 300",
    )
    daemon_parser.add_argument(
        "--page-size", type=int, default=100, help="Rows per page"
    )
    daemon_parser.add_argument(
        "--print-response",
        action="store_true",
        help="Print request payload and response JSON",
    )

    analyze_parser = subparsers.add_parser("analyze", help="Analyze synced order data")
    analyze_parser.add_argument(
        "--days", type=int, default=7, help="Analyze last N days"
    )

    dashboard_parser = subparsers.add_parser(
        "dashboard", help="Run abnormal orders dashboard"
    )
    dashboard_parser.add_argument(
        "--host", default="127.0.0.1", help="Dashboard host, default 127.0.0.1"
    )
    dashboard_parser.add_argument(
        "--port", type=int, default=8787, help="Dashboard port, default 8787"
    )
    dashboard_parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Background sync interval seconds, default 300",
    )
    dashboard_parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Background sync page size, default 100",
    )

    return parser.parse_args()


def run_daemon(
    config: Config, interval_seconds: int, page_size: int, print_response: bool
) -> None:
    print(f"Polling started. Sync every {interval_seconds} seconds")
    while True:
        date_text = dt.datetime.now().strftime("%Y-%m-%d")
        try:
            page_count, inserted = sync_once(
                config,
                date_text,
                date_text,
                page_size,
                print_response=print_response,
            )
            print(
                f"[{dt.datetime.now().isoformat(timespec='seconds')}] Sync success: pages={page_count}, new_records={inserted}"
            )
        except Exception as exc:
            print(
                f"[{dt.datetime.now().isoformat(timespec='seconds')}] Sync failed: {exc}"
            )
        time.sleep(interval_seconds)


def main() -> None:
    args = parse_args()

    if args.command == "analyze":
        db_path = args.db or os.getenv("JD_DB_PATH", "jd_orders.db")
        analyze_data(db_path, args.days)
        return

    if args.command == "dashboard":
        db_path = args.db or os.getenv("JD_DB_PATH", "jd_orders.db")
        run_dashboard(db_path, args.host, args.port, args.interval, args.page_size)
        return

    config = load_config(args)

    if args.command == "sync":
        today = dt.datetime.now().strftime("%Y-%m-%d")
        start_date = today
        end_date = today
        page_count, inserted = sync_once(
            config,
            start_date,
            end_date,
            args.page_size,
            print_response=args.print_response,
        )
        print(
            f"Sync done: pages={page_count}, new_records={inserted}, db={config.db_path}"
        )
        return

    if args.command == "daemon":
        run_daemon(
            config,
            args.interval,
            args.page_size,
            print_response=args.print_response,
        )
        return

    raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
