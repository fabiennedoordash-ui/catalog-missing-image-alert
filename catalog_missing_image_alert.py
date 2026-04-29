#!/usr/bin/env python3
"""
NV Catalog Missing-Image Alert

Runs daily at ~1 PM EST.

Flow:
1. Runs a single merged Trino query in Mode that joins:
     - MSIDs currently missing images in merchant_catalog_snapshot
     - BSKU rows where the merchant has sent an image URL in the last 2 days
   The query returns rows that satisfy both conditions (already joined).
2. Filters out broken/placeholder URLs:
     a. Pattern filter: drops known placeholder patterns (DD_PLACEHOLDER, ItemDefault,
        coming-soon, CVS weekly-ad, Modisoft hash URLs, etc.)
     b. HEAD probe: hits each remaining URL with a HEAD request to confirm it's
        actually live (200 OK). Skipped automatically when row count is high
        (one-time backfills) to keep runtime sane.
3. Posts a summary + CSVs to #nv-catalog-missingimage-alert.
"""

import os
import sys
import time
import tempfile
import concurrent.futures
from datetime import datetime
from io import StringIO
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ============================= CONFIG =============================

MODE_WORKSPACE = "doordash"
MODE_TOKEN = os.environ["MODE_TOKEN"]
MODE_SECRET = os.environ["MODE_SECRET"]
AUTH = (MODE_TOKEN, MODE_SECRET)

MISSING_IMAGE_REPORT_TOKEN = os.environ["MISSING_IMAGE_REPORT_TOKEN"]

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = "C0AUXV98TD1"  # #nv-catalog-missingimage-alert

# Slack user to tag as the action owner (will be @-mentioned at the top of msg)
SLACK_ACTION_OWNER_ID = "U0AR5NG6JRF"

# Slack users to cc at the bottom of the message
SLACK_CC_USER_IDS = ["U049N4SU51A", "U02ET47GB16", "U02NGA5G1GV"]

# How many merchants to show individually in the Slack breakdown before
# rolling the rest up into "+ N other merchants"
TOP_N_MERCHANTS = 15

BULK_TOOL_1_URL = "https://unity.doordash.com/suites/bulk/bulk_tools/categories/retail_catalog/fetch_photo_metadata"
BULK_TOOL_2_URL = "https://unity.doordash.com/suites/bulk/bulk_tools/categories/retail_catalog/update_product_item"

# ---------- URL FILTER CONFIG ----------
# If matched-row count exceeds this, the HEAD-probe step is SKIPPED and only the
# pattern filter runs. Probing 1M+ URLs would take hours; for backfills we accept
# that some links will fail in the bulk tool itself.
HEAD_PROBE_MAX_ROWS = 10_000_000

# Number of concurrent HEAD requests
HEAD_PROBE_CONCURRENCY = 300

# Per-request timeout in seconds for HEAD probe
HEAD_PROBE_TIMEOUT = 15

# Substring patterns (case-insensitive) that mark a URL as a known placeholder.
# Add new ones here when you spot them in bulk-tool failure reports.
PLACEHOLDER_SUBSTRINGS = [
    "dd_placeholder",
    "itemdefault",
    "coming-soon",
    "missing-image",
    "no-image",
    "noimage",
    "/placeholder",
    "comingsoon",
    "image-not-found",
]

# Hostname/path fragments for sources known to serve dead links reliably.
# Modisoft's `/doordash/images/` URLs are extension-less hashes that 404.
KNOWN_BAD_URL_FRAGMENTS = [
    "back2.modisoft.com/doordash/images/",
    "cvs.com/webcontent/images/weeklyad/",
]

# ============================= MODE API ===========================

def trigger_mode_run(report_token: str) -> str:
    url = f"https://app.mode.com/api/{MODE_WORKSPACE}/reports/{report_token}/runs"
    resp = requests.post(url, auth=AUTH, json={"parameters": {}})
    resp.raise_for_status()
    return resp.json()["token"]


def wait_for_run(report_token: str, run_token: str, max_wait_minutes: int = 45) -> None:
    url = f"https://app.mode.com/api/{MODE_WORKSPACE}/reports/{report_token}/runs/{run_token}"
    deadline = time.time() + (max_wait_minutes * 60)
    while time.time() < deadline:
        resp = requests.get(url, auth=AUTH)
        resp.raise_for_status()
        state = resp.json()["state"]
        if state == "succeeded":
            return
        if state in ("failed", "cancelled"):
            raise RuntimeError(f"Mode run {state} for report {report_token}")
        print(f"   ... waiting ({state})")
        time.sleep(20)
    raise TimeoutError(f"Mode run timed out for report {report_token}")


def fetch_run_csv(report_token: str, run_token: str) -> pd.DataFrame:
    qruns_url = (
        f"https://app.mode.com/api/{MODE_WORKSPACE}/reports/{report_token}"
        f"/runs/{run_token}/query_runs"
    )
    qruns = requests.get(qruns_url, auth=AUTH).json()
    qrun_token = qruns["_embedded"]["query_runs"][0]["token"]

    csv_url = (
        f"https://app.mode.com/api/{MODE_WORKSPACE}/reports/{report_token}"
        f"/runs/{run_token}/query_runs/{qrun_token}/results/content.csv"
    )
    resp = requests.get(csv_url, auth=AUTH)
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text), dtype=str)


def run_mode_report(report_token: str, label: str) -> pd.DataFrame:
    print(f"\n▶ Running Mode report: {label}")
    run_token = trigger_mode_run(report_token)
    print(f"   run_token = {run_token}")
    wait_for_run(report_token, run_token)
    df = fetch_run_csv(report_token, run_token)
    print(f"   → {len(df):,} rows returned")
    return df


# ============================= URL FILTERS ========================

def apply_pattern_filter(df: pd.DataFrame, url_col: str = "image_url") -> pd.DataFrame:
    """Drop rows whose URL matches a known-broken pattern. Always cheap to run."""
    before = len(df)
    url_lower = df[url_col].astype(str).str.lower()

    placeholder_re = "|".join(PLACEHOLDER_SUBSTRINGS)
    bad_pattern = url_lower.str.contains(placeholder_re, regex=True, na=False)

    bad_fragment = pd.Series(False, index=df.index)
    for frag in KNOWN_BAD_URL_FRAGMENTS:
        bad_fragment = bad_fragment | url_lower.str.contains(frag, regex=False, na=False)

    # Also drop URLs containing whitespace, quotes, or angle brackets (corrupt entries)
    bad_chars = df[url_col].astype(str).str.contains(r"[\s\"'<>]", regex=True, na=False)

    bad_mask = bad_pattern | bad_fragment | bad_chars
    dropped = bad_mask.sum()
    if dropped:
        print(f"   Pattern filter dropped {dropped:,} of {before:,} rows "
              f"({dropped / before * 100:.1f}%)")
    return df[~bad_mask].copy()


# Known placeholder strings/bytes returned by retailer image servers when the
# requested SKU image doesn't exist. These come back as HTTP 200 (so HEAD probe
# alone can't catch them). We download the first ~4KB of each URL and check the
# response body for these patterns.
PLACEHOLDER_RESPONSE_PATTERNS = [
    b"unable to find image",   # Albertsons (images.albertsons-media.com)
    b"image not found",
    b"no image available",
    b"image unavailable",
    b"<code>accessdenied",     # AWS S3 (e.g. cdn.localexpress.io)
    b'"status":404',           # JSON-formatted 404 (e.g. gmfgcdn.azureedge.net)
]

# Hosts that return placeholder PAGES (HTML) instead of images when the SKU
# isn't found — content-type starting with text/* on these hosts is a strong
# signal it's a placeholder.
PLACEHOLDER_HTML_HOSTS = {
    "images.albertsons-media.com",
}


def _probe_one_url(url: str) -> tuple:
    """Return (status_code, is_placeholder).

    Performs a ranged GET for the first 4KB so we can:
      1. Confirm the URL is reachable (status code)
      2. Inspect the response body for placeholder patterns
      3. Check content-type — if it's text/* on an image host, that's
         a placeholder response

    Returns (0, False) on connection failure.
    """
    try:
        # Ranged GET: 4KB is enough to either contain a placeholder text
        # response or the header bytes of a real image
        r = requests.get(
            url,
            timeout=HEAD_PROBE_TIMEOUT,
            stream=True,
            headers={"Range": "bytes=0-4095"},
            allow_redirects=True,
        )
        status = r.status_code
        if not (200 <= status < 300):
            r.close()
            return (status, False)

        ct = (r.headers.get("content-type") or "").lower()
        body = r.raw.read(4096) if r.raw else b""
        r.close()

        # Heuristic 1: text/* on a known image host = placeholder
        try:
            host = url.split("/")[2].lower()
        except IndexError:
            host = ""
        if host in PLACEHOLDER_HTML_HOSTS and ct.startswith("text/"):
            return (status, True)

        # Heuristic 2: response body contains known placeholder string
        body_lower = body.lower()
        for pat in PLACEHOLDER_RESPONSE_PATTERNS:
            if pat in body_lower:
                return (status, True)

        return (status, False)
    except Exception:
        return (0, False)  # DNS failure, timeout, etc.


def apply_head_probe(df: pd.DataFrame, url_col: str = "image_url") -> pd.DataFrame:
    """Probe each URL with a ranged GET; keep only rows where:
      - status is 2xx, AND
      - response body doesn't match a known placeholder pattern.

    Skipped if df has more than HEAD_PROBE_MAX_ROWS rows (saves runtime on backfills).
    """
    n = len(df)
    if n == 0:
        return df
    if n > HEAD_PROBE_MAX_ROWS:
        print(f"   Skipping HEAD probe: {n:,} rows exceeds threshold of "
              f"{HEAD_PROBE_MAX_ROWS:,}. Using pattern filter only.")
        return df

    print(f"   Probing {n:,} URLs with {HEAD_PROBE_CONCURRENCY} concurrent workers "
          f"(checking status + placeholder content)...")
    start = time.time()

    urls = df[url_col].tolist()
    with concurrent.futures.ThreadPoolExecutor(max_workers=HEAD_PROBE_CONCURRENCY) as ex:
        results = list(ex.map(_probe_one_url, urls))

    statuses = [r[0] for r in results]
    is_placeholder = [r[1] for r in results]

    df = df.copy()
    df["_http_status"] = statuses
    df["_is_placeholder"] = is_placeholder

    status_ok = df["_http_status"].between(200, 299)
    not_placeholder = ~df["_is_placeholder"]
    alive_mask = status_ok & not_placeholder

    alive = df[alive_mask].drop(columns=["_http_status", "_is_placeholder"])
    dead_status = (~status_ok).sum()
    dead_placeholder = (status_ok & ~not_placeholder).sum()
    elapsed = time.time() - start
    print(f"   Probe done in {elapsed:.0f}s — kept {len(alive):,}, "
          f"dropped {dead_status:,} (bad status) + {dead_placeholder:,} (placeholder)")
    return alive


# ============================= SLACK ==============================

def post_slack_alert(message: str, files: list) -> None:
    client = WebClient(token=SLACK_BOT_TOKEN)
    file_uploads = [
        {"file": path, "filename": fn, "title": title}
        for path, fn, title in files
    ]
    try:
        client.files_upload_v2(
            channel=SLACK_CHANNEL,
            initial_comment=message,
            file_uploads=file_uploads,
        )
    except SlackApiError as e:
        print(f"❌ Slack API error: {e.response['error']}")
        raise


# ============================= MAIN ===============================

def main():
    # --- 1) Pull merged result (Trino, single query) ---
    matched = run_mode_report(
        MISSING_IMAGE_REPORT_TOKEN,
        "Merged missing-image + BSKU URL query"
    )
    matched.columns = [c.lower() for c in matched.columns]
    matched["business_id"] = matched["business_id"].astype(str)
    matched["msid"] = matched["msid"].astype(str)

    # Defensive cleanup on URL column (query already filters to http%, but belt-and-suspenders)
    matched = matched[matched["image_url"].notna()]
    matched["image_url"] = matched["image_url"].str.strip()
    matched = matched[matched["image_url"] != ""]
    matched = matched[matched["image_url"].str.startswith(("http://", "https://"))]

    print(f"\n✅ {len(matched):,} MSIDs missing images with a usable BSKU URL")

    if matched.empty:
        print("Nothing to alert on — exiting quietly.")
        return

    # --- 3.5) Filter out broken URLs ---
    print("\n▶ Filtering broken URLs")
    pre_filter_count = len(matched)
    pre_filter_df = matched.copy()
    matched = apply_pattern_filter(matched)
    matched = apply_head_probe(matched)
    print(f"   → {len(matched):,} clean URLs remaining")

    # Capture dropped rows for attachment
    kept_keys = set(zip(matched["business_id"], matched["msid"]))
    dropped_df = pre_filter_df[
        ~pre_filter_df.apply(
            lambda r: (r["business_id"], r["msid"]) in kept_keys, axis=1
        )
    ]
    broken_count = len(dropped_df)

    if matched.empty:
        print("All URLs filtered out — nothing to send.")
        return

    # --- 4) CSV 1: fetch_photo_metadata bulk tool input ---
    csv1 = pd.DataFrame({
        "businessId": matched["business_id"],
        "itemMerchantSuppliedId": matched["msid"],
        "URL": matched["image_url"],
        "angle": "FRONT",
        "source": "MX",
    })

    # --- 5) CSV 2: update_product_item template ---
    csv2 = pd.DataFrame({
        "businessId": matched["business_id"],
        "itemMerchantSuppliedId": matched["msid"],
        "photoID": "",
    })

    # --- 6) Per-merchant breakdown (business_name comes from the merged query) ---
    name_lookup = (
        matched[["business_id", "business_name"]]
        .dropna(subset=["business_name"])
        .drop_duplicates(subset=["business_id"])
        .set_index("business_id")["business_name"]
        .to_dict()
    )

    counts_matched = matched.groupby("business_id").size().sort_values(ascending=False)

    today = datetime.now(ZoneInfo("America/New_York")).strftime("%b %d, %Y")
    total_matched = len(matched)
    total_merchants = (counts_matched > 0).sum()

    lines = [
        f"*🖼️ Missing Image Catalog Update — {today}*",
        "",
        (
            f"<@{SLACK_ACTION_OWNER_ID}> — *{total_matched:,} MSIDs* across "
            f"*{total_merchants:,} Mx* have no image live but a usable URL in BSKU "
            f"was sent in the last 48Hr. Please run the bulk flow to push these live ASAP."
        ),
    ]

    if broken_count > 0:
        broken_pct = broken_count / pre_filter_count * 100
        lines += [
            "",
            (
                f"_Broken URL filter: {broken_count:,} of {pre_filter_count:,} matched URLs "
                f"({broken_pct:.1f}%) flagged as broken / placeholder and excluded — "
                f"see `broken_image_urls.csv`._"
            ),
        ]

    lines += [
        "",
        f"*Top {min(TOP_N_MERCHANTS, total_merchants)} Mx:*",
    ]

    top_merchants = counts_matched.head(TOP_N_MERCHANTS)
    for biz_id, matched_n in top_merchants.items():
        name = name_lookup.get(biz_id, f"Biz {biz_id}")
        lines.append(f"• *{name}* ({biz_id}) — *{matched_n:,}*")

    remaining = counts_matched.iloc[TOP_N_MERCHANTS:]
    if len(remaining) > 0:
        lines.append(
            f"• _+ {len(remaining):,} other Mx ({remaining.sum():,} MSIDs) — see attached CSV_"
        )

    cc_mentions = " ".join(f"<@{uid}>" for uid in SLACK_CC_USER_IDS)
    lines += [
        "",
        "*Steps:*",
        f"1. Upload `missing_image_urls.csv` → <{BULK_TOOL_1_URL}|fetch_photo_metadata>",
        f"2. Paste resulting photoIDs into `photo_id_template.csv` → <{BULK_TOOL_2_URL}|update_product_item>",
        "3. Confirm in thread 🧵",
        "",
        f"cc: {cc_mentions}",
    ]
    message = "\n".join(lines)

    # --- 7) Write CSVs and post to Slack ---
    with tempfile.TemporaryDirectory() as td:
        p1 = os.path.join(td, "missing_image_urls.csv")
        p2 = os.path.join(td, "photo_id_template.csv")
        csv1.to_csv(p1, index=False)
        csv2.to_csv(p2, index=False)

        files_to_upload = [
            (p1, "missing_image_urls.csv", "Bulk Tool 1 Input (fetch_photo_metadata)"),
            (p2, "photo_id_template.csv",  "Bulk Tool 2 Template (update_product_item)"),
        ]

        if broken_count > 0:
            p3 = os.path.join(td, "broken_image_urls.csv")
            broken_csv = pd.DataFrame({
                "businessId": dropped_df["business_id"],
                "itemMerchantSuppliedId": dropped_df["msid"],
                "URL": dropped_df["image_url"],
            })
            broken_csv.to_csv(p3, index=False)
            files_to_upload.append(
                (p3, "broken_image_urls.csv", "Broken / placeholder URLs excluded from upload")
            )

        post_slack_alert(message, files=files_to_upload)
    print("✅ Slack message posted")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Alert failed: {e}", file=sys.stderr)
        raise
