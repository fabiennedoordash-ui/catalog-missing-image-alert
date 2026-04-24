#!/usr/bin/env python3
"""
NV Catalog Missing-Image Alert

Runs daily at 1 PM EST.

Flow:
1. Pulls MSIDs currently missing images from merchant_catalog via Mode (Snowflake).
2. Pulls BSKU items that have image URLs for the same business_ids via Mode (Trino).
3. Joins to find which missing-image MSIDs have a URL available in BSKU.
4. Posts a summary + two CSVs to #nv-catalog-missingimage-alert:
     - missing_image_urls.csv  (input for fetch_photo_metadata bulk tool)
     - photo_id_template.csv   (template for update_product_item bulk tool)

Env vars required:
    MODE_TOKEN
    MODE_SECRET
    MISSING_IMAGE_REPORT_TOKEN   (Mode report token for Query 1 — Snowflake)
    BSKU_URLS_REPORT_TOKEN       (Mode report token for Query 2 — Trino)
    SLACK_BOT_TOKEN
"""

import os
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

MISSING_IMAGE_REPORT_TOKEN = os.environ["MISSING_IMAGE_REPORT_TOKEN"]  # Query 1
BSKU_URLS_REPORT_TOKEN = os.environ["BSKU_URLS_REPORT_TOKEN"]          # Query 2

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL = "nv-catalog-missingimage-alert"

# Merchant display names (ordered — appears in this order in the Slack message)
MERCHANT_NAMES = {
    "799015":   "Dollar General",
    "13434351": "Family Dollar",
}

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


# ============================= SLACK ==============================

def post_slack_alert(message: str, files: list) -> None:
    """files: list of (local_path, slack_filename, title) tuples"""
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
    # --- 1) Pull missing-image MSIDs (Snowflake) ---
    missing = run_mode_report(MISSING_IMAGE_REPORT_TOKEN, "Query 1 — missing images")
    missing.columns = [c.lower() for c in missing.columns]
    missing["business_id"] = missing["business_id"].astype(str)
    missing["msid"] = missing["msid"].astype(str)

    # --- 2) Pull BSKU items with URLs (Trino) ---
    bsku = run_mode_report(BSKU_URLS_REPORT_TOKEN, "Query 2 — BSKU URLs")
    bsku.columns = [c.lower() for c in bsku.columns]
    bsku["business_id"] = bsku["business_id"].astype(str)
    bsku["msid"] = bsku["msid"].astype(str)

    # keep latest row per (biz, msid)
    if "updated_at" in bsku.columns:
        bsku = (
            bsku.sort_values("updated_at")
                .drop_duplicates(subset=["business_id", "msid"], keep="last")
        )
    # only items with a real URL
    bsku = bsku[bsku["image_url"].notna() & (bsku["image_url"].str.strip() != "")]

    # --- 3) Inner join — missing-image MSIDs that have a URL in BSKU ---
    matched = missing.merge(
        bsku[["business_id", "msid", "image_url"]],
        on=["business_id", "msid"],
        how="inner",
    )
    print(f"\n✅ Matched {len(matched):,} MSIDs with URLs available in BSKU")

    if matched.empty:
        print("Nothing to alert on — exiting quietly.")
        return

    # --- 4) CSV 1: fetch_photo_metadata bulk tool input ---
    csv1 = pd.DataFrame({
        "businessId": matched["business_id"],
        "itemMerchantSuppliedId": matched["msid"],
        "URL": matched["image_url"],
        "angle": "FRONT",
        "source": "MX",
    })

    # --- 5) CSV 2: update_product_item template (photoID blank) ---
    csv2 = pd.DataFrame({
        "businessId": matched["business_id"],
        "itemMerchantSuppliedId": matched["msid"],
        "photoID": "",
    })

    # --- 6) Per-merchant breakdown ---
    counts_matched = matched.groupby("business_id").size().to_dict()
    counts_missing = missing.groupby("business_id").size().to_dict()

    today = datetime.now(ZoneInfo("America/New_York")).strftime("%b %d, %Y")
    total_matched = len(matched)
    total_merchants = sum(1 for b in MERCHANT_NAMES if counts_matched.get(b, 0) > 0)

    lines = [
        f"*🖼️ Missing Image Catalog Update — {today}*",
        "",
        (
            f"*{total_matched:,} MSIDs* across *{total_merchants} merchant"
            f"{'s' if total_merchants != 1 else ''}* currently have no image live "
            f"in the catalog but *do* have an image URL available in BSKU. "
            f"Please run the bulk flow below to get these images live ASAP."
        ),
        "",
        "*Mx Breakdown:*",
    ]
    for biz_id, display_name in MERCHANT_NAMES.items():
        matched_n = counts_matched.get(biz_id, 0)
        missing_n = counts_missing.get(biz_id, 0)
        pct = (matched_n / missing_n * 100) if missing_n else 0
        lines.append(
            f"• *{display_name}* ({biz_id}) — *{matched_n:,} MSIDs* "
            f"({pct:.1f}% of this Mx's missing-image MSIDs) found in BSKU"
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
        csv1.to_csv(p1, index=False)
        csv2.to_csv(p2, index=False)
        post_slack_alert(
            message,
            files=[
                (p1, "missing_image_urls.csv", "Bulk Tool 1 Input (fetch_photo_metadata)"),
                (p2, "photo_id_template.csv",  "Bulk Tool 2 Template (update_product_item)"),
            ],
        )
    print("✅ Slack message posted")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Alert failed: {e}", file=sys.stderr)
        raise
