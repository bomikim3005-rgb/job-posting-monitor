"""Job Posting Monitor - Streamlit app using the Adzuna API.

Run:
    pip install -r requirements.txt
    export ADZUNA_APP_ID=your_id
    export ADZUNA_APP_KEY=your_key
    streamlit run app.py

With no keys set, the app runs on built-in sample data so you can test the UI.
"""
import os
import re

import pandas as pd
import requests
import streamlit as st

API_URL = "https://api.adzuna.com/v1/api/jobs/us/search/{page}"

# SAMPLE = [
#     {"title": "Principal People Analytics Partner", "company": {"display_name": "Acme Corp"},
#      "location": {"display_name": "San Francisco, CA"}, "salary_min": 180000, "salary_max": 240000,
#      "created": "2026-11-20T10:00:00Z", "redirect_url": "https://example.com/1",
#      "description": "SQL, Python, Tableau, workforce planning, forecasting."},
#     {"title": "Senior Talent Analytics Manager", "company": {"display_name": "Globex"},
#      "location": {"display_name": "Remote"}, "salary_min": 150000, "salary_max": 190000,
#      "created": "2026-11-19T10:00:00Z", "redirect_url": "https://example.com/2",
#      "description": "Recruiting funnel metrics, Power BI, SQL, stakeholder management."},
#     {"title": "Data Analyst, HR Systems", "company": {"display_name": "Initech"},
#      "location": {"display_name": "New York, NY"}, "salary_min": None, "salary_max": None,
#      "created": "2026-11-18T10:00:00Z", "redirect_url": "https://example.com/3",
#      "description": "Workday reporting, SQL, dashboards."},
# ]


# How the keywords are sent to Adzuna. "what" (any word) was the original
# behavior and returned noise like "Event Manager" for "people analytics".
MATCH_MODES = {
    "Exact phrase": "what_phrase",
    "All words": "what_and",
    "Any word": "what",
}


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_jobs(what: str, match: str, where: str, pages: int, app_id: str, app_key: str):
    """Return a list of raw job dicts. Falls back to sample data without keys."""
    if not app_id or not app_key:
        return SAMPLE, True
    results = []
    for page in range(1, pages + 1):
        resp = requests.get(
            API_URL.format(page=page),
            params={
                "app_id": app_id,
                "app_key": app_key,
                "results_per_page": 50,
                MATCH_MODES[match]: what,
                "where": where,
                "sort_by": "date",
                "content-type": "application/json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        batch = resp.json().get("results", [])
        if not batch:
            break
        results.extend(batch)
    return results, False


def to_frame(raw: list) -> pd.DataFrame:
    rows = []
    for j in raw:
        rows.append({
            "title": j.get("title", ""),
            "company": (j.get("company") or {}).get("display_name", ""),
            "location": (j.get("location") or {}).get("display_name", ""),
            "salary_min": j.get("salary_min"),
            "salary_max": j.get("salary_max"),
            "posted": pd.to_datetime(j.get("created"), errors="coerce"),
            "url": j.get("redirect_url", ""),
            "description": j.get("description", ""),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["title"] = df["title"].apply(lambda t: re.sub(r"<.*?>", "", t))
        df = df.drop_duplicates(subset=["title", "company", "location"])
    return df


def title_matches(title: str, what: str, match: str) -> bool:
    """Apply the same match mode to the job title only."""
    t, words = title.lower(), what.lower().split()
    if not words:
        return True
    if match == "Exact phrase":
        return " ".join(words) in " ".join(t.split())
    if match == "All words":
        return all(w in t for w in words)
    return any(w in t for w in words)


st.set_page_config(page_title="Job Posting Monitor", layout="wide")
st.title("Job Posting Monitor")

with st.sidebar:
    what = st.text_input("Keywords", "people analytics")
    match = st.radio("Match", list(MATCH_MODES), horizontal=True)
    title_only = st.checkbox("Keywords must appear in job title", value=False)
    where = st.text_input("Location", "California")
    pages = st.slider("Pages (50 results each)", 1, 5, 2)
    must_have = st.text_input("Must include in description (comma separated)", "")
    min_salary = st.number_input("Minimum salary (USD, 0 = ignore)", 0, 500000, 0, step=10000)
    # Never pre-fill these from the environment: on a public deployment the
    # values would be sent to every visitor's browser.
    app_id = st.text_input("Adzuna app id (optional)", "")
    app_key = st.text_input("Adzuna app key (optional)", "", type="password")

# Visitor's own keys if given, otherwise the server's (env vars / Streamlit secrets).
app_id = app_id or os.getenv("ADZUNA_APP_ID", "")
app_key = app_key or os.getenv("ADZUNA_APP_KEY", "")

try:
    raw, is_sample = fetch_jobs(what, match, where, pages, app_id, app_key)
except requests.HTTPError as e:
    # Don't print the exception itself: its message includes the request URL,
    # which contains app_key.
    st.error(f"API request failed (HTTP {e.response.status_code}).")
    st.stop()
except requests.RequestException as e:
    st.error(f"API request failed ({type(e).__name__}).")
    st.stop()

if is_sample:
    st.info("No API keys found - showing sample data.")

df = to_frame(raw)
if df.empty:
    st.warning("No results. Try different keywords or location.")
    st.stop()

if title_only:
    before = len(df)
    df = df[df["title"].apply(lambda t: title_matches(t, what, match))]
    st.caption(f"Title filter kept {len(df)} of {before} postings.")

for term in [t.strip().lower() for t in must_have.split(",") if t.strip()]:
    df = df[df["description"].str.lower().str.contains(term, na=False)]
if min_salary:
    df = df[(df["salary_max"].fillna(0) >= min_salary)]

c1, c2, c3 = st.columns(3)
c1.metric("Postings", len(df))
c2.metric("Companies", df["company"].nunique())
with_salary = df.dropna(subset=["salary_min", "salary_max"])
c3.metric("Median salary midpoint",
          f"${((with_salary['salary_min'] + with_salary['salary_max']) / 2).median():,.0f}"
          if not with_salary.empty else "n/a")

left, right = st.columns(2)
left.subheader("Top hiring companies")
left.bar_chart(df["company"].value_counts().head(10))
right.subheader("Postings by day")
right.line_chart(df.dropna(subset=["posted"]).groupby(df["posted"].dt.date).size())

st.subheader("Postings")
st.dataframe(
    df.drop(columns=["description"]).sort_values("posted", ascending=False),
    column_config={"url": st.column_config.LinkColumn("link")},
    width="stretch",
    hide_index=True,
)
st.download_button("Download CSV", df.to_csv(index=False), "jobs.csv", "text/csv")
