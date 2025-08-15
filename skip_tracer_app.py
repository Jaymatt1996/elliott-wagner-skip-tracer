"""
Elliott Wagner Solutions — Skip Tracing App (Streamlit)
======================================================

Purpose:
  • Batch-enrich a list of owners with up to 3 living relatives/executors using TruthFinder (PeopleConnect) via RapidAPI.
  • Export results to Google Sheets automatically (one worksheet per run) and allow CSV download.

⚠️ Compliance & Use:
  • This app is NOT FCRA-compliant. Do not use for employment, housing, or credit decisions.
  • Use for real estate lead gen, estate outreach, and personal research only.

How to Deploy (quick):
  1) Create a new private Google Sheet and copy its URL. Share the sheet with your Google Service Account email (created in step 2) as Editor.
  2) Create a Google Cloud Service Account with a JSON key. (Role: Editor is fine for Sheets.)
  3) On Streamlit Cloud: create a new app, then add these secrets (Settings → Secrets):

     [secrets]
     RAPIDAPI_KEY = "<your RapidAPI key>"
     RAPIDAPI_HOST = "<the X-RapidAPI-Host value from the API's Endpoints page>"
     RAPIDAPI_URL = "<the full endpoint URL from RapidAPI Endpoints tab>"  # e.g., https://<provider>.p.rapidapi.com/people/search
     RAPIDAPI_METHOD = "GET"  # or POST if your endpoint requires it
     RATE_LIMIT_PER_MIN = 45  # adjust to your plan limits

     GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/........"
     GOOGLE_SERVICE_ACCOUNT_JSON = "<paste the entire service account JSON on ONE line>"

     BRAND_NAME = "Elliott Wagner Solutions"
     BRAND_SLOGAN = "Rejuvenating & Restoring"

  4) Requirements (add to requirements.txt):
     streamlit
     requests
     pandas
     tenacity
     gspread
     google-auth

  5) Push this file as app.py and deploy.

Notes on RapidAPI:
  • You *must* send both headers: X-RapidAPI-Key and X-RapidAPI-Host, and call the correct RAPIDAPI_URL from the API Endpoints tab.
  • If the endpoint requires POST, set RAPIDAPI_METHOD="POST" and the app will send JSON.
  • Because vendor schemas vary, this app extracts relatives/contacts from common keys ("relatives", "possibleRelatives", "associated_people", etc.). If your specific endpoint uses different keys, adjust RELATIVE_KEYS / field paths below.

"""
import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ---------------------------
# Config & Secrets
# ---------------------------

def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    return st.secrets.get(name, os.getenv(name, default))

APP_BRAND = get_secret("BRAND_NAME", "Elliott Wagner Solutions")
APP_SLOGAN = get_secret("BRAND_SLOGAN", "Rejuvenating & Restoring")
RAPIDAPI_KEY = get_secret("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = get_secret("RAPIDAPI_HOST", "")
RAPIDAPI_URL = get_secret("RAPIDAPI_URL", "")
RAPIDAPI_METHOD = (get_secret("RAPIDAPI_METHOD", "GET") or "GET").upper()
RATE_LIMIT_PER_MIN = int(get_secret("RATE_LIMIT_PER_MIN", 45))

GOOGLE_SHEET_URL = get_secret("GOOGLE_SHEET_URL", "")
SERVICE_JSON_RAW = get_secret("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# ---------------------------
# UI
# ---------------------------

st.set_page_config(page_title=f"{APP_BRAND} — Skip Tracer", page_icon="🕵️", layout="wide")

st.markdown(f"""
# 🕵️ {APP_BRAND} — Skip Tracer
*{APP_SLOGAN}*

Upload a CSV or Excel with owners. We'll enrich with up to **3 living relatives/executors** per lead via TruthFinder (RapidAPI) and export to Google Sheets.

**Compliance:** Not FCRA compliant. Do **not** use for employment, housing, or credit decisions.
""")

with st.expander("🔧 Connection Status & Setup Hints", expanded=False):
    st.write("**RapidAPI URL**:", RAPIDAPI_URL or "❌ not set")
    st.write("**X-RapidAPI-Host**:", RAPIDAPI_HOST or "❌ not set")
    st.write("**Method**:", RAPIDAPI_METHOD)
    st.write("**Google Sheet**:", GOOGLE_SHEET_URL or "❌ not set")
    st.caption("Tip: Find the exact endpoint URL and Host on the RapidAPI ‘Endpoints’ tab for the TruthFinder API you subscribed to.")

# ---------------------------
# Helpers — Google Sheets
# ---------------------------

def get_gspread_client():
    if not SERVICE_JSON_RAW:
        return None
    try:
        info = json.loads(SERVICE_JSON_RAW)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Google Service Account JSON invalid: {e}")
        return None


def export_to_gsheet(df: pd.DataFrame, sheet_url: str, title_prefix: str = "SkipTrace") -> Optional[str]:
    gc = get_gspread_client()
    if not gc:
        return None
    try:
        sh = gc.open_by_url(sheet_url)
        ts = datetime.now().strftime("%Y-%m-%d_%H%M")
        ws_title = f"{title_prefix}_{ts}"
        ws = sh.add_worksheet(title=ws_title, rows=str(len(df) + 10), cols=str(len(df.columns) + 5))
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
        return ws_title
    except Exception as e:
        st.error(f"Failed to export to Google Sheets: {e}")
        return None

# ---------------------------
# Helpers — RapidAPI / TruthFinder
# ---------------------------

class RapidAPIError(Exception):
    pass


def _headers() -> Dict[str, str]:
    return {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(RapidAPIError))
def call_truthfinder(params: Dict[str, Any]) -> Dict[str, Any]:
    if not (RAPIDAPI_KEY and RAPIDAPI_HOST and RAPIDAPI_URL):
        raise RapidAPIError("RapidAPI credentials/URL not configured.")

    try:
        if RAPIDAPI_METHOD == "POST":
            r = requests.post(RAPIDAPI_URL, headers=_headers(), json=params, timeout=60)
        else:
            r = requests.get(RAPIDAPI_URL, headers=_headers(), params=params, timeout=60)
    except requests.RequestException as e:
        raise RapidAPIError(f"Network error: {e}")

    if r.status_code != 200:
        # Some RapidAPI providers return details in JSON
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RapidAPIError(f"HTTP {r.status_code}: {detail}")

    try:
        return r.json()
    except Exception as e:
        raise RapidAPIError(f"Invalid JSON response: {e}")


# Flexible extraction: many providers use different keys for contacts/relatives
RELATIVE_KEYS = [
    "relatives", "possibleRelatives", "associated_people", "associates", "relationships"
]
PHONE_KEYS = ["phones", "phone_numbers", "phoneNumbers", "telephones"]
EMAIL_KEYS = ["emails", "email_addresses", "emailAddresses"]
DECEASED_KEYS = ["is_deceased", "deceased", "deceased_flag", "isDeceased"]
RELATION_NAME_KEYS = ["name", "full_name", "fullName"]
RELATIONSHIP_LABEL_KEYS = ["relationship", "relation", "type", "label", "role"]


def _first_truthy(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def flatten_contacts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    contacts: List[Dict[str, Any]] = []

    # try top-level known areas
    candidates: List[Any] = []
    for k in RELATIVE_KEYS:
        v = payload.get(k)
        if isinstance(v, list):
            candidates.extend(v)

    # scan recursively if top-level not present
    if not candidates:
        stack = [payload]
        visited = set()
        while stack:
            cur = stack.pop()
            if id(cur) in visited:
                continue
            visited.add(id(cur))
            if isinstance(cur, dict):
                for k, v in cur.items():
                    if k in RELATIVE_KEYS and isinstance(v, list):
                        candidates.extend(v)
                    elif isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(cur, list):
                stack.extend(cur)

    for rel in candidates:
        if not isinstance(rel, dict):
            continue
        name = _first_truthy(rel, RELATION_NAME_KEYS, default="")
        relationship = _first_truthy(rel, RELATIONSHIP_LABEL_KEYS, default="relative")
        is_deceased = bool(_first_truthy(rel, DECEASED_KEYS, default=False))
        phones = _first_truthy(rel, PHONE_KEYS, default=[]) or []
        emails = _first_truthy(rel, EMAIL_KEYS, default=[]) or []

        # Normalize phones/emails to strings
        def norm_list(x):
            out = []
            if isinstance(x, list):
                for i in x:
                    if isinstance(i, dict):
                        num = i.get("number") or i.get("phone") or i.get("value")
                        if num:
                            out.append(str(num))
                    elif i not in (None, ""):
                        out.append(str(i))
            elif isinstance(x, dict):
                v = x.get("number") or x.get("value") or x.get("phone")
                if v:
                    out.append(str(v))
            elif x not in (None, ""):
                out.append(str(x))
            return out

        contacts.append({
            "name": name,
            "relationship": relationship,
            "is_deceased": is_deceased,
            "phones": norm_list(phones),
            "emails": norm_list(emails),
        })

    return contacts


RELATIONSHIP_PRIORITY = {
    # Higher = more priority
    "executor": 100,
    "personal representative": 95,
    "representative": 90,
    "trustee": 90,
    "attorney": 85,
    "spouse": 80,
    "wife": 80,
    "husband": 80,
    "partner": 70,
    "child": 70,
    "son": 70,
    "daughter": 70,
    "sibling": 60,
    "brother": 60,
    "sister": 60,
    "parent": 60,
    "mother": 60,
    "father": 60,
    "aunt": 50,
    "uncle": 50,
    "cousin": 40,
    "relative": 30,
    "associate": 20,
}


def contact_score(c: Dict[str, Any]) -> float:
    label = str(c.get("relationship", "")).lower()
    score = RELATIONSHIP_PRIORITY.get(label, 25)
    # Bonus for having a phone number
    if c.get("phones"):
        score += 10
    # Bonus for email
    if c.get("emails"):
        score += 5
    return score


def select_top_contacts(contacts: List[Dict[str, Any]], limit: int = 3, living_only: bool = True) -> List[Dict[str, Any]]:
    filtered = [c for c in contacts if (not living_only or not c.get("is_deceased"))]
    ranked = sorted(filtered, key=contact_score, reverse=True)
    deduped = []
    seen = set()
    for c in ranked:
        key = (c.get("name"), tuple(c.get("phones", [])[:1]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)
        if len(deduped) >= limit:
            break
    return deduped


def rate_limit_sleep(last_call_ts: float, rate_per_min: int) -> float:
    if rate_per_min <= 0:
        return time.time()
    min_interval = 60.0 / float(rate_per_min)
    now = time.time()
    delta = now - last_call_ts
    if delta < min_interval:
        time.sleep(min_interval - delta)
        now = time.time()
    return now

# ---------------------------
# Input Section
# ---------------------------

st.subheader("1) Upload your list")
input_file = st.file_uploader("CSV or Excel with owners (columns like first_name, last_name, address, city, state, etc.)", type=["csv", "xlsx"]) 


def read_input(file) -> pd.DataFrame:
    if file is None:
        return pd.DataFrame()
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    else:
        return pd.read_excel(file)

source_df = read_input(input_file)

if not source_df.empty:
    st.dataframe(source_df.head(10))
else:
    st.info("Upload a file to begin.")

st.subheader("2) Map columns for the search query")
col1, col2, col3, col4 = st.columns(4)
with col1:
    first_col = st.selectbox("First Name column", options=["<none>"] + list(source_df.columns), index=0)
with col2:
    last_col = st.selectbox("Last Name column", options=["<none>"] + list(source_df.columns), index=0)
with col3:
    city_col = st.selectbox("City column (optional)", options=["<none>"] + list(source_df.columns), index=0)
with col4:
    state_col = st.selectbox("State column (optional)", options=["<none>"] + list(source_df.columns), index=0)

st.subheader("3) Options")
limit_per_lead = st.number_input("Relatives per lead (max)", min_value=1, max_value=3, value=3, step=1)
only_living = st.checkbox("Only return living contacts", value=True)

start_btn = st.button("🚀 Run Skip Trace")

# ---------------------------
# Processing
# ---------------------------

def build_query(row: pd.Series) -> Dict[str, Any]:
    q: Dict[str, Any] = {}
    if first_col != "<none>" and pd.notna(row.get(first_col)):
        q["first_name"] = str(row[first_col]).strip()
    if last_col != "<none>" and pd.notna(row.get(last_col)):
        q["last_name"] = str(row[last_col]).strip()
    if city_col != "<none>" and pd.notna(row.get(city_col)):
        q["city"] = str(row[city_col]).strip()
    if state_col != "<none>" and pd.notna(row.get(state_col)):
        q["state"] = str(row[state_col]).strip()
    return q


def enrich_row(row: pd.Series) -> Dict[str, Any]:
    query = build_query(row)
    if not query:
        return {"error": "Missing name fields"}
    payload = call_truthfinder(query)
    contacts = flatten_contacts(payload)
    top = select_top_contacts(contacts, limit=limit_per_lead, living_only=only_living)

    out: Dict[str, Any] = {}
    for i in range(limit_per_lead):
        if i < len(top):
            c = top[i]
            out[f"contact{i+1}_name"] = c.get("name", "")
            out[f"contact{i+1}_relationship"] = c.get("relationship", "")
            out[f"contact{i+1}_is_living"] = "No" if c.get("is_deceased") else "Yes"
            out[f"contact{i+1}_phone"] = ", ".join(c.get("phones", [])[:2])
            out[f"contact{i+1}_email"] = ", ".join(c.get("emails", [])[:2])
        else:
            out[f"contact{i+1}_name"] = ""
            out[f"contact{i+1}_relationship"] = ""
            out[f"contact{i+1}_is_living"] = ""
            out[f"contact{i+1}_phone"] = ""
            out[f"contact{i+1}_email"] = ""
    return out


if start_btn:
    if source_df.empty:
        st.error("Please upload a file first.")
        st.stop()
    if not (RAPIDAPI_KEY and RAPIDAPI_HOST and RAPIDAPI_URL):
        st.error("RapidAPI secrets not configured.")
        st.stop()

    progress = st.progress(0)
    results: List[Dict[str, Any]] = []
    last_ts = 0.0

    for idx, row in source_df.iterrows():
        # Rate limit
        last_ts = rate_limit_sleep(last_ts, RATE_LIMIT_PER_MIN)
        try:
            enriched = enrich_row(row)
        except RapidAPIError as e:
            enriched = {"error": str(e)}
        results.append(enriched)
        progress.progress(int(((idx + 1) / len(source_df)) * 100))

    out_df = pd.concat([source_df.reset_index(drop=True), pd.DataFrame(results)], axis=1)

    st.subheader("Results Preview")
    st.dataframe(out_df.head(50))

    # Download CSV
    csv_bytes = out_df.to_csv(index=False).encode("utf-8")
    st.download_button("💾 Download CSV", data=csv_bytes, file_name=f"skiptrace_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv")

    # Export to Google Sheets
    if GOOGLE_SHEET_URL and SERVICE_JSON_RAW:
        ws_title = export_to_gsheet(out_df, GOOGLE_SHEET_URL)
        if ws_title:
            st.success(f"Exported to Google Sheets worksheet: {ws_title}")
    else:
        st.info("Set GOOGLE_SHEET_URL and GOOGLE_SERVICE_ACCOUNT_JSON in secrets to enable Google Sheets export.")

st.caption("Built for Elliott Wagner Solutions — Streamlit app for batch skip tracing via RapidAPI.")
