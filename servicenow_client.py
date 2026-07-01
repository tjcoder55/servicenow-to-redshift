import requests
import config


def _flatten(value):
    """ServiceNow reference fields return dicts — extract display_value if present."""
    if isinstance(value, dict):
        return value.get("display_value", "")
    return value


def fetch_all_records(table: str = None, page_size: int = 1000) -> list:
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

    print(f"Total records fetched from '{table}': {len(all_records)}")
    return all_records
