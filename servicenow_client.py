from typing import Optional
import requests
import config


def _flatten(value):
    """ServiceNow reference fields return dicts — extract display_value if present."""
    if isinstance(value, dict):
        return value.get("display_value", "")
    return value


def fetch_all_records(table: str = None, since: str = None, page_size: int = 1000) -> list:
    """
    Fetch records from a ServiceNow table.
    since: ISO timestamp string (e.g. '2026-01-01 00:00:00'). If provided, only
           records with sys_updated_on >= since are returned. If None, all records
           are fetched (full load).
    """
    table = table or config.SN_TABLE
    url   = f"{config.SN_BASE_URL}/api/now/table/{table}"
    auth  = (config.SN_USERNAME, config.SN_PASSWORD)
    headers = {"Accept": "application/json"}

    all_records = []
    offset = 0

    while True:
        params = {
            "sysparm_limit":         page_size,
            "sysparm_offset":        offset,
            "sysparm_display_value": "true",
            "sysparm_exclude_reference_link": "true",
        }
        if since:
            params["sysparm_query"] = f"sys_updated_on>={since}"

        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()

        batch = response.json().get("result", [])
        if not batch:
            break

        all_records.extend({k: _flatten(v) for k, v in record.items()} for record in batch)
        print(f"  Fetched {len(all_records)} records...")

        if len(batch) < page_size:
            break
        offset += page_size

    mode = f"since {since}" if since else "full load"
    print(f"Total records fetched from '{table}' ({mode}): {len(all_records)}")
    return all_records


def get_max_updated_on(records: list) -> Optional[str]:
    """
    Returns the highest sys_updated_on timestamp from a batch of records.
    This becomes the new high-water mark saved to the Airflow Variable after a
    successful run, so the next run only pulls records updated after this point.
    Returns None if the batch is empty.
    """
    timestamps = [r["sys_updated_on"] for r in records if r.get("sys_updated_on")]
    return max(timestamps) if timestamps else None
