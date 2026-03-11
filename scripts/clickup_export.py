#!/usr/bin/env python3
import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

CLICKUP_API_BASE = "https://api.clickup.com/api/v2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export processed CSV files to a ClickUp task as attachments."
    )
    parser.add_argument("--list-id", required=True, help="ClickUp List ID where export task will be created")
    parser.add_argument("--status-name", default="exported zisson data", help="Target task status name")
    parser.add_argument("--csv-dir", default="data/processed", help="Directory containing processed CSV files")
    parser.add_argument(
        "--name-template",
        default="Zisson export {period}",
        help="Task name template. Available placeholders: {period}, {timestamp}",
    )
    parser.add_argument(
        "--period-granularity",
        choices=["month", "week"],
        default="month",
        help="Controls {period} value in task name",
    )
    parser.add_argument("--description", default="", help="Optional task description")
    parser.add_argument("--allow-existing-task", action="store_true", help="Reuse existing task with same name if found")
    parser.add_argument("--dry-run", action="store_true", help="Plan actions but do not call ClickUp API")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _must_env(key: str) -> str:
    value = os.getenv(key, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


def build_headers() -> Dict[str, str]:
    token = _must_env("CLICKUP_API_TOKEN")
    prefix = os.getenv("CLICKUP_AUTH_PREFIX", "").strip()
    auth_value = f"{prefix} {token}".strip() if prefix else token
    return {
        "Authorization": auth_value,
        "Content-Type": "application/json",
    }


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    timeout_seconds: int = 60,
    max_attempts: int = 5,
) -> dict:
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout_seconds,
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"Retryable HTTP {response.status_code}: {response.text[:500]}")
            response.raise_for_status()
            if not response.text:
                return {}
            return response.json()
        except Exception as exc:
            if attempt >= max_attempts:
                raise RuntimeError(f"Request failed after {max_attempts} attempts: {exc}") from exc
            sleep_s = min(30, (2 ** attempt))
            logging.warning("Request attempt %s failed: %s. Retrying in %ss", attempt, exc, sleep_s)
            time.sleep(sleep_s)
    return {}


def period_label(now: datetime, granularity: str) -> str:
    if granularity == "month":
        return now.strftime("%Y-%m")
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def get_csv_files(csv_dir: Path) -> List[Path]:
    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV directory not found: {csv_dir}")
    files = sorted(p for p in csv_dir.glob("*.csv") if p.is_file())
    if not files:
        raise RuntimeError(f"No CSV files found in {csv_dir}")
    return files


def get_list_info(session: requests.Session, headers: Dict[str, str], list_id: str) -> dict:
    return request_json(session, "GET", f"{CLICKUP_API_BASE}/list/{list_id}", headers)


def resolve_status(list_info: dict, target_status_name: str) -> Optional[str]:
    statuses = list_info.get("statuses") or []
    target = target_status_name.strip().lower()
    for status in statuses:
        name = str(status.get("status", "")).strip().lower()
        if name == target:
            return status.get("status")
    return None


def find_existing_task_id(session: requests.Session, headers: Dict[str, str], list_id: str, task_name: str) -> Optional[str]:
    page = 0
    while True:
        data = request_json(
            session,
            "GET",
            f"{CLICKUP_API_BASE}/list/{list_id}/task",
            headers,
            params={"page": page, "include_closed": "true"},
        )
        tasks = data.get("tasks") or []
        if not tasks:
            return None
        for task in tasks:
            if str(task.get("name", "")).strip() == task_name:
                return str(task.get("id"))
        if len(tasks) < 100:
            return None
        page += 1


def create_task(
    session: requests.Session,
    headers: Dict[str, str],
    list_id: str,
    task_name: str,
    status_name: Optional[str],
    description: str,
) -> str:
    payload = {
        "name": task_name,
        "description": description,
    }
    if status_name:
        payload["status"] = status_name

    data = request_json(
        session,
        "POST",
        f"{CLICKUP_API_BASE}/list/{list_id}/task",
        headers,
        json_body=payload,
    )
    task_id = str(data.get("id", "")).strip()
    if not task_id:
        raise RuntimeError("Task creation succeeded but no task id was returned")
    return task_id


def upload_attachment(
    session: requests.Session,
    auth_header_value: str,
    task_id: str,
    file_path: Path,
    max_attempts: int = 5,
) -> None:
    url = f"{CLICKUP_API_BASE}/task/{task_id}/attachment"

    for attempt in range(1, max_attempts + 1):
        try:
            with file_path.open("rb") as f:
                response = session.post(
                    url,
                    headers={"Authorization": auth_header_value},
                    files={"attachment": (file_path.name, f, "text/csv")},
                    timeout=120,
                )
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"Retryable HTTP {response.status_code}: {response.text[:500]}")
            response.raise_for_status()
            return
        except Exception as exc:
            if attempt >= max_attempts:
                raise RuntimeError(f"Failed uploading {file_path.name} after {max_attempts} attempts: {exc}") from exc
            sleep_s = min(30, (2 ** attempt))
            logging.warning("Upload attempt %s failed for %s: %s. Retrying in %ss", attempt, file_path.name, exc, sleep_s)
            time.sleep(sleep_s)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    csv_files = get_csv_files(Path(args.csv_dir))
    now = datetime.now(timezone.utc)
    period = period_label(now, args.period_granularity)
    timestamp = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    task_name = args.name_template.format(period=period, timestamp=timestamp)

    description = args.description.strip()
    if not description:
        description = (
            f"Automated export of processed Zisson queue CSV files.\\n"
            f"Period label: {period}\\n"
            f"Generated at (UTC): {timestamp}\\n"
            f"Files attached: {len(csv_files)}"
        )

    logging.info("Prepared export task name: %s", task_name)
    logging.info("CSV files to upload: %d", len(csv_files))

    if args.dry_run:
        logging.info("Dry run enabled; no ClickUp API calls will be made.")
        for file_path in csv_files:
            logging.info("Would upload: %s", file_path)
        return

    headers = build_headers()
    session = requests.Session()

    list_info = get_list_info(session, headers, args.list_id)
    matched_status = resolve_status(list_info, args.status_name)
    if matched_status is None:
        available = [str(s.get("status", "")) for s in (list_info.get("statuses") or [])]
        logging.warning("Status '%s' not found in list. Available statuses: %s", args.status_name, available)
        logging.warning("Task will be created with ClickUp default status.")

    task_id: Optional[str] = None
    if args.allow_existing_task:
        task_id = find_existing_task_id(session, headers, args.list_id, task_name)
        if task_id:
            logging.info("Reusing existing task id=%s", task_id)

    if not task_id:
        task_id = create_task(
            session=session,
            headers=headers,
            list_id=args.list_id,
            task_name=task_name,
            status_name=matched_status,
            description=description,
        )
        logging.info("Created task id=%s", task_id)

    auth_header_value = headers["Authorization"]
    for file_path in csv_files:
        upload_attachment(session, auth_header_value, task_id, file_path)
        logging.info("Uploaded attachment: %s", file_path.name)

    summary = {
        "task_id": task_id,
        "task_name": task_name,
        "files_uploaded": [p.name for p in csv_files],
        "exported_at_utc": timestamp,
    }
    logging.info("Export complete: %s", json.dumps(summary))


if __name__ == "__main__":
    main()
