# Databricks notebook source
# MAGIC %md
# MAGIC # Shared GBFS ingestion helpers
# MAGIC
# MAGIC Pulled into the ingestion notebooks with `%run ./_gbfs_common`, so a retry fix or
# MAGIC a path change only has one home.

# COMMAND ----------

import json, os, time
from datetime import datetime, timezone
import requests

CATALOG = "citibike"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"
BASE    = "https://gbfs.lyft.com/gbfs/1.1/bkn/en"

# GBFS nests records under data.<something> and the key changes per feed.
RECORD_KEYS = ("stations", "regions", "bikes", "alerts", "plans")

# COMMAND ----------

def fetch(url, retries=3, backoff=2):
    """Fetch a feed with exponential backoff. Raises if it never succeeds."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            return response.json()
        except Exception as error:
            if attempt == retries:
                raise
            print(f"  attempt {attempt} failed ({type(error).__name__}), retrying in {backoff}s")
            time.sleep(backoff)
            backoff *= 2


def count_records(payload):
    """Return (key, count) for whichever collection this feed carries."""
    data = payload.get("data", {})
    for key in RECORD_KEYS:
        if key in data:
            return key, len(data[key])
    return None, 0


def land(feed_name):
    """Fetch a feed and write it raw to the volume, partitioned by UTC capture date."""
    url = f"{BASE}/{feed_name}.json"
    captured_at = datetime.now(timezone.utc)
    payload = fetch(url)

    # Wrap, don't modify: capture metadata goes around the payload so the original
    # document stays reprocessable if the schema changes later.
    envelope = {
        "captured_at": captured_at.isoformat(),
        "source_url": url,
        "payload": payload,
    }

    folder = f"{LANDING}/{feed_name}/dt={captured_at:%Y-%m-%d}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{captured_at:%Y%m%dT%H%M%SZ}.json"

    with open(path, "w") as file:
        json.dump(envelope, file)

    key, count = count_records(payload)
    print(f"OK  {feed_name:22} {count:>5} {key or 'records'} -> {path}")
    return path
