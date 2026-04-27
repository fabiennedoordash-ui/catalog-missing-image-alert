#!/usr/bin/env python3
"""
NV Catalog Missing-Image Alert
Runs daily at ~1 PM EST.

Flow:
1. Pulls matched MSIDs (missing image in catalog + URL available in BSKU)
   from a single merged Trino Mode report.
2. Filters out URLs that match known placeholder/broken patterns.
3. Posts a summary + three CSVs to #nv-catalog-missingimage-alert:
     - missing_image_urls.csv     (Bulk Tool 1 input — usable URLs only)
     - photo_id_template.csv      (Bulk Tool 2 template — usable URLs only)
     - broken_image_urls.csv      (rejected URLs, for tracking only)
"""
import os
import re
import sys
import time
import tempfile
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

# NOTE: Migrated from two queries to a single merged Trino query.
# The BSKU_URLS_REPORT_TOKEN secret now points to the merged report.
# The old MISSING_IMAGE_REPORT_TOKEN secret is no longer used and can be deleted.
MATCHED_REPORT_TOKEN = os.environ["BSKU_URLS_REPORT_TOKEN"]

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = "C0AUXV98TD1"  # #nv-catalog-missingimage-alert

TOP_N_MERCHANTS = 15

BULK_TOOL_1_URL = "https://unity.doordash.com/suites/bulk/bulk_tools/categories/retail_catalog/fetch_photo_metadata"
BULK_TOOL_2_URL = "https://unity.doordash.com/suites/bulk/bulk_tools/categories/retail_catalog/update_product_item"

# ============================= MODE API ===========================
def trigger_mode_run(report_token: str) -> str:
    url = f"https://app.mode.com/api/{MODE_WORKSPACE}/reports/{report_token}/runs"
    resp = requests.post(url, auth=AUTH, json={"parameters": {}})
    resp.raise_for_status()
    return resp.json()["token"]

def wait_for_run(report_token: str, run_token: str, max_wait_minutes: int = 25) -> None:
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

# ============================= URL CLEANING ======================
# Rules derived from manually-labeled sample (~300 rows):
# 96% recall, 96% precision on the labeled set.
def is_placeholder_url(url) -> bool:
    """Return True if URL matches a known placeholder/broken pattern."""
    if not isinstance(url, str):
        return True
    u = url.lower()

    # 1) place-hold.it — text-on-color stub images (100% bad in sample)
    if "place-hold.it" in u:
        return True
    # 2) liquorapps "Uncat_Icon" — generic uncategorized icon
    if "uncat_icon" in u:
        return True
    # 3) liquorapps /wp/bg/ — non-product background images
    if "liquorapps.com/wp/bg/" in u:
        return True
    # 4) Sobeys _PHP_VOILA_ / _GPHP_VOILA_ — placeholder thumbnail variant
    #    (real Sobeys URLs use plain _VOILA_ instead)
    if re.search(r"_p?gphp_voila_|_php_voila_", u):
        return True
    # 5) Meijer URLs missing the _A1C1_ infix — older/legacy style, usually broken
    if "meijer.com/content/dam/meijer/product/" in u and "_a1c1_" not in u:
        return True
    # 6) localexpress legacy import path
    if "localexpress.io/original/img/import/" in u:
        return True
    # 7) bevz syndigo-images bucket
    if "bevz-media" in u and "syndigo-images" in u:
        return True
    # 8) Generic catch-all kept from prior version
    if "default_image_url" in u:
        return True
    return False

def clean_image_urls(df: pd.DataFrame, url_col: str = "image_url"):
    """Split a DataFrame into (good_df, broken_df) based on URL patterns."""
    bad_mask = df[url_col].apply(is_placeholder_url)
    return df.loc[~bad_mask].copy(), df.loc[bad_mask].copy()

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
    # --- 1) Pull merged matched dataset (single Trino query) ---
    raw = run_mode_report(MATCHED_REPORT_TOKEN, "Matched missing-image MSIDs + BSKU URLs")
    raw.columns = [c.lower() for c in raw.columns]

    # Defensive type / shape coercion
    raw["business_id"] = raw["business_id"].astype(str)
    raw["msid"] = raw["msid"].astype(str)
    raw["image_url"] = raw["image_url"].astype(str).str.strip()

    # Belt-and-suspenders: keep only http(s) URLs even though the SQL filters this
    raw = raw[raw["image_url"].str.startswith(("http://", "https://"))]

    total_raw = len(raw)
    print(f"\nTotal matched rows from Mode: {total_raw:,}")

    if raw.empty:
        print("Nothing to alert on — exiting quietly.")
        return

    # --- 2) Split into usable URLs vs broken/placeholder URLs ---
    matched, broken = clean_image_urls(raw)
    n_good = len(matched)
    n_broken = len(broken)
    pct_broken = (n_broken / total_raw * 100) if total_raw else 0.0
    print(f"  → {n_good:,} usable URLs ({100 - pct_broken:.1f}%)")
    print(f"  → {n_broken:,} broken/placeholder URLs filtered out ({pct_broken:.1f}%)")

    if matched.empty:
        print("All URLs were filtered as broken — exiting quietly.")
        return

    # --- 3) CSV 1: fetch_photo_metadata bulk tool input ---
    csv1 = pd.DataFrame({
        "businessId": matched["business_id"],
        "itemMerchantSuppliedId": matched["msid"],
        "URL": matched["image_url"],
        "angle": "FRONT",
        "source": "MX",
    })

    # --- 4) CSV 2: update_product_item template ---
    csv2 = pd.DataFrame({
        "businessId": matched["business_id"],
        "itemMerchantSuppliedId": matched["msid"],
        "photoID": "",
    })

    # --- 5) CSV 3: broken URLs for tracking (NOT for upload) ---
    broken_cols = [
        "business_id", "business_name", "msid", "item_name",
        "brand", "category_l1", "category_l2", "photo_id", "image_url",
    ]
    csv3 = broken[[c for c in broken_cols if c in broken.columns]].copy()
    csv3 = csv3.rename(columns={
        "business_id": "Business ID",
        "business_name": "Business Name",
        "msid": "MSID",
        "item_name": "Item Name",
        "brand": "Brand",
        "category_l1": "Category L1",
        "category_l2": "Category L2",
        "photo_id": "Photo ID",
        "image_url": "Broken Image URL",
    })

    # --- 6) Per-merchant breakdown ---
    counts_matched = matched.groupby("business_id").size().sort_values(ascending=False)
    counts_broken = (
        broken.groupby("business_id").size()
        if not broken.empty
        else pd.Series(dtype=int)
    )

    name_lookup = (
        pd.concat([matched, broken])[["business_id", "business_name"]]
        .dropna(subset=["business_name"])
        .drop_duplicates(subset=["business_id"])
        .set_index("business_id")["business_name"]
        .to_dict()
    )

    today = datetime.now(ZoneInfo("America/New_York")).strftime("%b %d, %Y")
    total_merchants = (counts_matched > 0).sum()

    lines = [
        f"*🖼️ Missing Image Catalog Update — {today}*",
        "",
        (
            f"*{n_good:,} MSIDs* across *{total_merchants} merchant"
            f"{'s' if total_merchants != 1 else ''}* currently have no image live "
            f"in the catalog but *do* have a usable image URL available in BSKU. "
            f"Please run the bulk flow below to get these images live ASAP."
        ),
        "",
        (
            f"*Broken URL filter:* {n_broken:,} of {total_raw:,} matched URLs "
            f"({pct_broken:.1f}%) were flagged as broken / placeholder images "
            f"and excluded from the upload — see `broken_image_urls.csv` for the full list."
        ),
        "",
        f"*Top {min(TOP_N_MERCHANTS, total_merchants)} Mx by Volume:*",
    ]

    top = counts_matched.head(TOP_N_MERCHANTS)
    for biz_id, matched_n in top.items():
        name = name_lookup.get(biz_id, f"Biz {biz_id}")
        broken_n = int(counts_broken.get(biz_id, 0))
        broken_suffix = f" ({broken_n:,} broken links)" if broken_n else ""
        lines.append(
            f"• *{name}* ({biz_id}) — *{matched_n:,} usable MSIDs*{broken_suffix}"
        )

    remaining = counts_matched.iloc[TOP_N_MERCHANTS:]
    if len(remaining) > 0:
        lines.append(
            f"• _+ {len(remaining)} other merchants ({remaining.sum():,} usable MSIDs) — "
            f"see attached CSV for the full list_"
        )

    lines += [
        "",
        "*Upload Steps:*",
        f"1. Download `missing_image_urls.csv` (attached) and upload to the fetch_photo_metadata "
        f"bulk tool: {BULK_TOOL_1_URL}",
        f"2. Download the photoId output from Step 1, paste the photoIds into "
        f"`photo_id_template.csv` (attached), and upload to the update_product_item bulk tool: {BULK_TOOL_2_URL}",
        "3. Confirm the final bulk upload has processed in thread below 🧵",
    ]

    message = "\n".join(lines)

    # --- 7) Write CSVs and post to Slack ---
    with tempfile.TemporaryDirectory() as td:
        p1 = os.path.join(td, "missing_image_urls.csv")
        p2 = os.path.join(td, "photo_id_template.csv")
        p3 = os.path.join(td, "broken_image_urls.csv")
        csv1.to_csv(p1, index=False)
        csv2.to_csv(p2, index=False)
        csv3.to_csv(p3, index=False)

        files = [
            (p1, "missing_image_urls.csv", "Bulk Tool 1 Input (fetch_photo_metadata)"),
            (p2, "photo_id_template.csv",  "Bulk Tool 2 Template (update_product_item)"),
        ]
        if n_broken > 0:
            files.append(
                (p3, "broken_image_urls.csv", f"Broken / Placeholder URLs ({n_broken:,} rows)")
            )

        post_slack_alert(message, files=files)

    print("✅ Slack message posted")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Alert failed: {e}", file=sys.stderr)
        raise
