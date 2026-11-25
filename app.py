"""Job Posting Monitor - Streamlit app.

Two tabs:
  Market trends  Reads the dbt models the scheduled pipeline builds: BigQuery when
                 BQ_PROJECT and GCP_SA_KEY are set, otherwise the local DuckDB file
                 data/job_market.duckdb that ./pipeline/run.sh creates.
  Live search    Calls the Adzuna API directly, only when someone clicks Search.

Secrets (Streamlit Cloud: app Settings -> Secrets; locally: env vars):
  ADZUNA_APP_ID, ADZUNA_APP_KEY   live search
  BQ_PROJECT, GCP_SA_KEY          market trends (GCP_SA_KEY = service account JSON)

Run locally:
    pip install -r requirements.txt
    streamlit run app.py
"""
import json
import os
import re
from datetime import timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import requests
import streamlit as st

API_URL = "https://api.adzuna.com/v1/api/jobs/us/search/{page}"
LOCAL_DB = Path(__file__).resolve().parent / "data" / "job_market.duckdb"
BQ_DATASET = "job_market"

# How the keywords are sent to Adzuna. "what" (any word) was the original
# behavior and returned noise like "Event Manager" for "people analytics".
MATCH_MODES = {
    "Exact phrase": "what_phrase",
    "All words": "what_and",
    "Any word": "what",
}

# dbt models read by the Market trends tab ({name} is replaced per warehouse).
QUERIES = {
    "postings": """
        select job_id, title, company, location, salary_min, salary_max,
               salary_is_predicted, posted_salary_mid, posted_at, url,
               first_seen_at, last_seen_at
        from {dim_postings}""",
    "roles": "select job_id, role from {bridge_posting_roles}",
    "daily": "select posted_date, role, postings from {fct_daily_postings}",
    "skills": "select skill, role, postings_mentioning from {mart_skill_mentions}",
}
TABLES = ["dim_postings", "bridge_posting_roles", "fct_daily_postings", "mart_skill_mentions"]


# ---------------------------------------------------------------- helpers

def role_label(role: str) -> str:
    return role.replace("_", " ").capitalize()


def naive_utc(s: pd.Series) -> pd.Series:
    """Timestamps as tz-naive UTC, whichever warehouse they came from."""
    s = pd.to_datetime(s)
    return s.dt.tz_convert(None) if s.dt.tz is not None else s


def ranked_bar(df: pd.DataFrame, value: str, label: str, title: str, fmt: str = ","):
    """Horizontal bar chart sorted by value (st.bar_chart sorts alphabetically)."""
    if df.empty:
        st.caption("No data yet.")
        return
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X(f"{value}:Q", title=title, axis=alt.Axis(format=fmt)),
        y=alt.Y(f"{label}:N", sort="-x", title=None),
        tooltip=[alt.Tooltip(f"{label}:N"), alt.Tooltip(f"{value}:Q", format=fmt)],
    )
    st.altair_chart(chart, width="stretch")


def salary_text(row) -> str:
    lo, hi = row.salary_min, row.salary_max
    if pd.isna(lo) and pd.isna(hi):
        return ""
    lo = hi if pd.isna(lo) else lo
    hi = lo if pd.isna(hi) else hi
    text = f"${lo / 1000:,.0f}k" if lo == hi else f"${lo / 1000:,.0f}k–${hi / 1000:,.0f}k"
    return text + (" (est.)" if row.salary_is_predicted else "")


# ---------------------------------------------------------- market trends

@st.cache_data(ttl=900, show_spinner="Loading pipeline data...")
def load_marts():
    """Return ({name: DataFrame}, source label), or (None, None) if not configured."""
    project, key = os.getenv("BQ_PROJECT"), os.getenv("GCP_SA_KEY")
    if project and key:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_info(json.loads(key))
        client = bigquery.Client(project=project, credentials=creds)
        names = {t: f"`{project}.{BQ_DATASET}.{t}`" for t in TABLES}
        return {k: client.query(q.format(**names)).to_dataframe(create_bqstorage_client=False)
                for k, q in QUERIES.items()}, "BigQuery"
    if LOCAL_DB.exists():
        import duckdb

        with duckdb.connect(str(LOCAL_DB), read_only=True) as con:
            return {k: con.sql(q.format(**{t: t for t in TABLES})).df()
                    for k, q in QUERIES.items()}, "local DuckDB"
    return None, None


def render_trends():
    try:
        data, source = load_marts()
    except Exception as e:  # show the kind of failure, never credentials
        st.error(f"Couldn't load pipeline data ({type(e).__name__}). "
                 "Check the BQ_PROJECT and GCP_SA_KEY secrets.")
        return
    if data is None:
        st.info("Not connected to pipeline data yet. Set BQ_PROJECT and GCP_SA_KEY "
                "for BigQuery, or run ./pipeline/run.sh locally.")
        return

    postings, roles = data["postings"].copy(), data["roles"]
    daily, skills = data["daily"].copy(), data["skills"]
    if postings.empty:
        st.info("The pipeline hasn't collected any postings yet.")
        return
    for col in ("posted_at", "first_seen_at", "last_seen_at"):
        postings[col] = naive_utc(postings[col])
    daily["posted_date"] = naive_utc(daily["posted_date"])

    all_roles = sorted(roles["role"].unique())
    picked = st.multiselect("Roles", all_roles, default=all_roles, format_func=role_label)
    if not picked:
        st.warning("Pick at least one role.")
        return
    picked_roles = roles[roles["role"].isin(picked)]
    p = postings[postings["job_id"].isin(picked_roles["job_id"])]

    started, updated = postings["first_seen_at"].min(), postings["last_seen_at"].max()
    st.caption(f"Source: {source} · collected every 6 hours from Adzuna since "
               f"{started:%b} {started.day} · last update {updated:%b} {updated.day}, "
               f"{updated:%H:%M} UTC · San Francisco + 80 km")

    posted = p["posted_salary_mid"].dropna()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Postings tracked", f"{len(p):,}")
    c2.metric("Companies", f"{p['company'].nunique():,}")
    c3.metric("New in last 7 days", f"{(p['first_seen_at'] >= updated - timedelta(days=7)).sum():,}")
    c4.metric("Median posted salary", f"${posted.median():,.0f}" if len(posted) else "n/a",
              help=f"Midpoint of {len(posted)} employer-posted salary ranges. "
                   "Adzuna's estimated salaries are excluded.")

    left, right = st.columns(2)
    with left:
        st.subheader("New postings per day")
        d = daily[daily["role"].isin(picked)]
        wide = (d.pivot_table(index="posted_date", columns="role", values="postings",
                              aggfunc="sum", fill_value=0)
                 .rename(columns=role_label))
        # Blank out each role's days before its oldest collected posting: no postings
        # were collected for those days, which isn't the same as none being posted.
        wide = wide.where(wide.gt(0).cummax())
        if wide.empty:
            st.caption("Not enough data yet.")
        elif len(wide) == 1:
            # A line chart can't draw a single date, so it would look empty.
            st.bar_chart(wide, stack=False)
        else:
            st.line_chart(wide)
        st.caption("By the date each posting was created, for every posting the pipeline "
                   "has collected. Days before the first run only include postings "
                   "still listed then, so each line starts at the oldest posting "
                   "collected for that role.")
    with right:
        st.subheader("Top hiring companies")
        top = (p["company"].dropna().value_counts().head(10)
                .rename_axis("company").reset_index(name="postings"))
        ranked_bar(top, "postings", "company", "Postings")

    st.subheader("Skills mentioned")
    s = (skills[skills["role"].isin(picked)]
         .groupby("skill", as_index=False)["postings_mentioning"].sum())
    s["share"] = s["postings_mentioning"] / len(picked_roles)
    ranked_bar(s.nlargest(12, "share"), "share", "skill", "Share of postings", ".0%")
    st.caption("A lower bound: Adzuna only returns the first ~500 characters of each "
               "description, so skills listed further down are missed.")

    st.subheader("Postings")
    query = st.text_input("Filter by title or company", "", key="trends_filter")
    role_lists = (picked_roles.groupby("job_id")["role"]
                  .agg(lambda r: ", ".join(sorted(role_label(x) for x in r))).rename("roles"))
    t = p.merge(role_lists, on="job_id")
    if query:
        t = t[t["title"].str.contains(query, case=False, na=False, regex=False)
              | t["company"].str.contains(query, case=False, na=False, regex=False)]
    t = t.assign(salary=t.apply(salary_text, axis=1)).sort_values("posted_at", ascending=False)
    cols = ["title", "company", "location", "roles", "salary", "posted_at", "first_seen_at", "url"]
    st.dataframe(
        t[cols],
        column_config={
            "url": st.column_config.LinkColumn("link", display_text="open"),
            "posted_at": st.column_config.DatetimeColumn("posted", format="MMM D"),
            "first_seen_at": st.column_config.DatetimeColumn("first seen", format="MMM D, HH:mm"),
        },
        width="stretch",
        hide_index=True,
    )
    st.download_button("Download CSV", t[cols].to_csv(index=False), "postings.csv",
                       "text/csv", key="trends_csv")


# ------------------------------------------------------------ live search

@st.cache_data(ttl=3600, show_spinner="Searching Adzuna...")
def fetch_jobs(what: str, match: str, where: str, pages: int, app_id: str, app_key: str) -> list:
    """Return a list of raw job dicts from Adzuna (one API call per page)."""
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
    return results


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


def render_live():
    st.caption("Searches Adzuna directly. Each search uses one Adzuna API call per page, "
               "from the same monthly quota as the pipeline.")
    # A form, so Adzuna is only called on "Search", not on every page load.
    with st.form("live_search"):
        c1, c2, c0 = st.columns(3)
        what = c1.text_input("Keywords", "people analytics")
        where = c2.text_input("Location", "California")
        company = c0.text_input("Company (contains)", "", placeholder="e.g. LinkedIn",
                                help="Keeps postings whose company name contains this text, "
                                     "ignoring case. It filters the results Adzuna returns, "
                                     "so pair it with keywords, or raise Pages to see more.")
        match = st.radio("Match", list(MATCH_MODES), horizontal=True)
        title_only = st.checkbox("Keywords must appear in job title", value=False)
        c3, c4, c5 = st.columns(3)
        pages = c3.slider("Pages (50 results each)", 1, 3, 1)
        must_have = c4.text_input("Must include in description (comma separated)", "")
        min_salary = c5.number_input("Minimum salary (USD, 0 = ignore)", 0, 500000, 0, step=10000)
        with st.expander("Use your own Adzuna keys (optional)"):
            # Never pre-fill these from the environment: on a public deployment the
            # values would be sent to every visitor's browser.
            own_id = st.text_input("Adzuna app id", "")
            own_key = st.text_input("Adzuna app key", "", type="password")
        submitted = st.form_submit_button("Search Adzuna", type="primary")

    if submitted:
        # Visitor's own keys if given, otherwise the server's (env vars / Streamlit secrets).
        app_id = own_id or os.getenv("ADZUNA_APP_ID", "")
        app_key = own_key or os.getenv("ADZUNA_APP_KEY", "")
        if not app_id or not app_key:
            st.warning("Live search isn't configured: no Adzuna keys found.")
            return
        try:
            raw = fetch_jobs(what, match, where, pages, app_id, app_key)
        except requests.HTTPError as e:
            # Don't print the exception itself: its message includes the request URL,
            # which contains app_key.
            st.error(f"API request failed (HTTP {e.response.status_code}).")
            return
        except requests.RequestException as e:
            st.error(f"API request failed ({type(e).__name__}).")
            return
        st.session_state["live"] = {"raw": raw, "what": what, "match": match,
                                    "title_only": title_only, "must_have": must_have,
                                    "min_salary": min_salary, "company": company}

    state = st.session_state.get("live")
    if not state:
        st.info("Set your search and click **Search Adzuna**.")
        return

    df = to_frame(state["raw"])
    if df.empty:
        st.warning("No results. Try different keywords or location.")
        return
    if state["title_only"]:
        before = len(df)
        df = df[df["title"].apply(lambda t: title_matches(t, state["what"], state["match"]))]
        st.caption(f"Title filter kept {len(df)} of {before} postings.")
    company = state.get("company", "").strip()
    if company:
        before = len(df)
        df = df[df["company"].str.contains(company, case=False, na=False, regex=False)]
        st.caption(f"Company filter kept {len(df)} of {before} postings.")
    for term in [t.strip().lower() for t in state["must_have"].split(",") if t.strip()]:
        df = df[df["description"].str.lower().str.contains(term, na=False, regex=False)]
    if state["min_salary"]:
        df = df[df["salary_max"].fillna(0) >= state["min_salary"]]

    c1, c2, c3 = st.columns(3)
    c1.metric("Postings", len(df))
    c2.metric("Companies", df["company"].nunique())
    with_salary = df.dropna(subset=["salary_min", "salary_max"])
    c3.metric("Median salary midpoint",
              f"${((with_salary['salary_min'] + with_salary['salary_max']) / 2).median():,.0f}"
              if not with_salary.empty else "n/a")

    left, right = st.columns(2)
    with left:
        st.subheader("Top hiring companies")
        top = (df["company"].replace("", pd.NA).dropna().value_counts().head(10)
                 .rename_axis("company").reset_index(name="postings"))
        ranked_bar(top, "postings", "company", "Postings")
    with right:
        st.subheader("Postings by day")
        dated = df.dropna(subset=["posted"])
        if dated.empty:
            st.caption("No data yet.")
        else:
            st.line_chart(dated.groupby(dated["posted"].dt.date).size().rename("postings"))

    st.subheader("Postings")
    st.dataframe(
        df.drop(columns=["description"]).sort_values("posted", ascending=False),
        column_config={"url": st.column_config.LinkColumn("link", display_text="open")},
        width="stretch",
        hide_index=True,
    )
    st.download_button("Download CSV", df.to_csv(index=False), "jobs.csv", "text/csv",
                       key="live_csv")


# ------------------------------------------------------------------- page

st.set_page_config(page_title="Job Posting Monitor", layout="wide")
st.title("Job Posting Monitor")
st.caption("Data roles in the San Francisco Bay Area, tracked every 6 hours: "
           "Adzuna → BigQuery → dbt → Streamlit.")

trends_tab, live_tab = st.tabs(["Market trends", "Live search"])
with trends_tab:
    render_trends()
with live_tab:
    render_live()
