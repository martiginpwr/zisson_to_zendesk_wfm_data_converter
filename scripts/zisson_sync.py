#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

API_URL = "https://app2.zissoninteract.com/external-api/v1/report-api/queuereports/queue-details"
INTERVAL_MINUTES = 15


@dataclass
class QueueConfig:
    id_to_name: Dict[str, str]
    name_to_id: Dict[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch queue volume data from Zisson API and build per-queue CSV files."
    )
    parser.add_argument("--mode", choices=["backfill", "incremental", "test"], required=True)
    parser.add_argument("--start-date", help="UTC ISO date/time (inclusive), e.g. 2026-03-01T00:00:00Z")
    parser.add_argument("--end-date", help="UTC ISO date/time (exclusive), e.g. 2026-03-08T00:00:00Z")
    parser.add_argument("--chunk-days", type=int, default=7)
    parser.add_argument("--overlap-days", type=int, default=3)
    parser.add_argument("--retention-days", type=int, default=730)
    parser.add_argument(
        "--queues",
        default="",
        help="Comma-separated queue names or queue IDs. Empty means all configured queues.",
    )
    parser.add_argument("--queues-config", default="config/queues.json")
    parser.add_argument("--data-dir", default="data/processed")
    parser.add_argument("--state-file", default="state/sync_state.json")
    parser.add_argument("--source-timezone", default="UTC")
    parser.add_argument("--first-queue-report", action="store_true")
    parser.add_argument("--api-url", default=API_URL)
    parser.add_argument(
        "--skip-state-update",
        action="store_true",
        help="Do not update state file even when mode would normally do so.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_queue_config(path: Path) -> QueueConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    id_to_name = {item["id"]: item["name"] for item in data["queues"]}
    name_to_id = {item["name"]: item["id"] for item in data["queues"]}
    return QueueConfig(id_to_name=id_to_name, name_to_id=name_to_id)


def parse_iso_utc(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def floor_to_15(dt: datetime) -> datetime:
    minute = (dt.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES
    return dt.replace(minute=minute, second=0, microsecond=0)


def infer_safe_end(now_utc: datetime) -> datetime:
    # Avoid current open interval by moving one interval back.
    return floor_to_15(now_utc) - timedelta(minutes=INTERVAL_MINUTES)


def day_start_utc(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def parse_queue_selection(raw: str, cfg: QueueConfig) -> Dict[str, str]:
    if not raw.strip():
        return dict(cfg.id_to_name)

    selected: Dict[str, str] = {}
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for part in parts:
        if part in cfg.id_to_name:
            selected[part] = cfg.id_to_name[part]
            continue
        if part in cfg.name_to_id:
            qid = cfg.name_to_id[part]
            selected[qid] = part
            continue
        raise ValueError(f"Queue '{part}' not found in config (match by exact name or ID).")
    return selected


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 _-]", "", name)
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    cleaned = cleaned.strip("._")
    return cleaned or "unknown_queue"


def queue_desc_to_name(desc: str) -> str:
    # API often returns names like 'SE Aftersales (123379)'; strip trailing code in parentheses.
    return re.sub(r"\s*\([^)]*\)\s*$", "", desc).strip()


def parse_interval_start(interval: str, source_tz: ZoneInfo) -> datetime:
    match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s*-\s*\d{2}:\d{2}$", interval.strip())
    if not match:
        raise ValueError(f"Unsupported interval format: '{interval}'")

    local_start = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M").replace(tzinfo=source_tz)
    return local_start.astimezone(timezone.utc)


def is_supported_interval(interval: str) -> bool:
    value = interval.strip()
    if not value:
        return False
    if value.lower() == "total":
        return False
    return bool(re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}\s*-\s*\d{2}:\d{2}$", value))


def build_headers() -> Dict[str, str]:
    api_token = _must_env("ZISSON_API_TOKEN")
    auth_header = _opt_env("ZISSON_AUTH_HEADER", "Authorization")
    auth_prefix = _opt_env("ZISSON_AUTH_PREFIX", "Bearer")

    headers = {"Content-Type": "application/json"}
    headers[auth_header] = f"{auth_prefix} {api_token}".strip()
    return headers


def _must_env(key: str) -> str:
    import os

    value = os.getenv(key, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


def _opt_env(key: str, default: str) -> str:
    import os

    value = os.getenv(key)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def chunk_ranges(start: datetime, end: datetime, chunk_days: int) -> Iterable[Tuple[datetime, datetime]]:
    cur = start
    step = timedelta(days=chunk_days)
    while cur < end:
        nxt = min(cur + step, end)
        yield cur, nxt
        cur = nxt


def request_with_retries(
    session: requests.Session,
    api_url: str,
    headers: Dict[str, str],
    payload: dict,
    max_attempts: int = 5,
    timeout_seconds: int = 60,
) -> list:
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.post(api_url, headers=headers, json=payload, timeout=timeout_seconds)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"Retryable HTTP {response.status_code}: {response.text[:500]}")
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, list):
                raise ValueError("Unexpected API response shape: expected list")
            return body
        except Exception as exc:
            if attempt >= max_attempts:
                raise RuntimeError(f"API request failed after {max_attempts} attempts: {exc}") from exc
            sleep_s = min(30, (2 ** attempt) + random.uniform(0, 1.5))
            logging.warning("Request attempt %s failed: %s. Retrying in %.1fs", attempt, exc, sleep_s)
            time.sleep(sleep_s)
    return []


def load_existing_csv(path: Path) -> Dict[datetime, int]:
    if not path.exists():
        return {}

    rows: Dict[datetime, int] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("datetime"):
                continue
            dt = parse_iso_utc(row["datetime"])
            value = int(float(row.get("value", "0")))
            rows[dt] = value
    return rows


def write_queue_csv(path: Path, series: Dict[datetime, int], start: datetime, end: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["datetime", "value"])

        cur = start
        while cur < end:
            writer.writerow([format_iso_utc(cur), int(series.get(cur, 0))])
            cur += timedelta(minutes=INTERVAL_MINUTES)


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def fetch_points(
    session: requests.Session,
    api_url: str,
    headers: Dict[str, str],
    source_tz: ZoneInfo,
    queue_ids: List[str],
    queue_id_to_name: Dict[str, str],
    start: datetime,
    end: datetime,
    chunk_days: int,
    first_queue_report: bool,
) -> Dict[str, Dict[datetime, int]]:
    collected: Dict[str, Dict[datetime, int]] = {}

    for chunk_start, chunk_end in chunk_ranges(start, end, chunk_days):
        payload = {
            "startDate": format_iso_utc(chunk_start),
            "endDate": format_iso_utc(chunk_end),
            "groupBy": "qh",
            "queueList": queue_ids,
            "firstQueueReport": first_queue_report,
        }
        logging.info("Fetching %s to %s for %d queues", payload["startDate"], payload["endDate"], len(queue_ids))
        rows = request_with_retries(session, api_url, headers, payload)

        for row in rows:
            interval = str(row.get("interval", "")).strip()
            queue_desc = str(row.get("queueDesc", "")).strip()
            offered = int(row.get("offeredCalls", 0) or 0)

            if not interval or not queue_desc:
                continue
            if not is_supported_interval(interval):
                logging.debug("Skipping non-interval row for queue '%s': interval='%s'", queue_desc, interval)
                continue

            dt_utc = parse_interval_start(interval, source_tz)
            if dt_utc < start or dt_utc >= end:
                continue

            normalized_name = queue_desc_to_name(queue_desc)
            queue_name = normalized_name

            if normalized_name in queue_id_to_name.values():
                queue_name = normalized_name

            series = collected.setdefault(queue_name, {})
            series[dt_utc] = offered

        logging.info("Fetched %d rows for chunk", len(rows))

    return collected


def determine_window(args: argparse.Namespace, state: dict) -> Tuple[datetime, datetime]:
    now_utc = datetime.now(timezone.utc)

    if args.end_date:
        end = parse_iso_utc(args.end_date)
    else:
        end = infer_safe_end(now_utc)

    if args.mode in {"backfill", "test"}:
        if not args.start_date:
            raise ValueError("--start-date is required for backfill/test mode")
        start = parse_iso_utc(args.start_date)
        return start, end

    if args.mode == "incremental":
        retention_start = end - timedelta(days=args.retention_days)
        state_end = state.get("last_successful_end_utc")

        if args.start_date:
            start = parse_iso_utc(args.start_date)
        elif state_end:
            start = parse_iso_utc(state_end) - timedelta(days=args.overlap_days)
            if start < retention_start:
                start = retention_start
        else:
            start = retention_start

        return start, end

    raise ValueError(f"Unsupported mode: {args.mode}")


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    cfg = load_queue_config(Path(args.queues_config))
    selected_queues = parse_queue_selection(args.queues, cfg)
    queue_ids = list(selected_queues.keys())

    state_path = Path(args.state_file)
    data_dir = Path(args.data_dir)
    state = load_state(state_path) if args.mode == "incremental" else {}

    start, end = determine_window(args, state)
    if start >= end:
        raise ValueError(f"Invalid date window: start ({format_iso_utc(start)}) >= end ({format_iso_utc(end)})")

    source_tz = ZoneInfo(args.source_timezone)
    headers = build_headers()

    logging.info("Mode=%s start=%s end=%s", args.mode, format_iso_utc(start), format_iso_utc(end))
    logging.info("Selected queues: %s", ", ".join(selected_queues.values()))

    session = requests.Session()
    fetched = fetch_points(
        session=session,
        api_url=args.api_url,
        headers=headers,
        source_tz=source_tz,
        queue_ids=queue_ids,
        queue_id_to_name=selected_queues,
        start=start,
        end=end,
        chunk_days=args.chunk_days,
        first_queue_report=args.first_queue_report,
    )

    output_start = day_start_utc(start)

    for queue_name in sorted(selected_queues.values()):
        filename = sanitize_filename(queue_name) + ".csv"
        out_path = data_dir / filename

        if args.mode == "incremental":
            series = load_existing_csv(out_path)
        else:
            series = {}

        incoming = fetched.get(queue_name, {})
        series.update(incoming)

        if args.mode == "incremental":
            retention_start = day_start_utc(end - timedelta(days=args.retention_days))
            series = {dt: val for dt, val in series.items() if retention_start <= dt < end}
            write_queue_csv(out_path, series, retention_start, end)
        else:
            write_queue_csv(out_path, series, output_start, end)

        logging.info("Wrote %s", out_path)

    if args.mode == "incremental":
        new_state = {
            "last_successful_end_utc": format_iso_utc(end),
            "updated_at_utc": format_iso_utc(datetime.now(timezone.utc)),
        }
        if args.skip_state_update:
            logging.warning("State update disabled by --skip-state-update.")
        elif args.queues.strip():
            logging.warning("Queue subset run detected; skipping state update to protect global incremental cursor.")
        else:
            save_state(state_path, new_state)
            logging.info("Updated state file: %s", state_path)
    elif args.mode == "backfill":
        new_state = {
            "last_successful_end_utc": format_iso_utc(end),
            "updated_at_utc": format_iso_utc(datetime.now(timezone.utc)),
        }
        if args.skip_state_update:
            logging.warning("State update disabled by --skip-state-update.")
        elif args.queues.strip():
            logging.warning("Queue subset backfill detected; skipping global state update.")
        else:
            save_state(state_path, new_state)
            logging.info("Updated state file after backfill: %s", state_path)


if __name__ == "__main__":
    main()
