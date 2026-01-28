from __future__ import annotations

import json
from datetime import datetime, timedelta, date
from pathlib import Path
import requests

from airflow import DAG
from airflow.decorators import task

DELPHI_BASE = "https://api.delphi.cmu.edu/epidata"
RAW_ROOT = Path("/opt/airflow/data/raw/fluview")

REGIONS = "nat"
WEEKS_BACK = 12


def _week1_start_sunday(year: int) -> date:
    jan4 = date(year, 1, 4)
    return jan4 - timedelta(days=(jan4.weekday() + 1) % 7)


def epiweek_to_date(ew: int) -> date:
    return _week1_start_sunday(ew // 100) + timedelta(weeks=(ew % 100) - 1)


def mmwr_epiweek(d: date) -> int:
    start = _week1_start_sunday(d.year)
    return d.year * 100 + ((d - start).days // 7) + 1


def get_json(endpoint, params=None):
    r = requests.get(f"{DELPHI_BASE}/{endpoint}/", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


with DAG(
    dag_id="fluview_ingest_structured",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["fluview", "bronze"],
) as dag:

    @task
    def ingest(ds: str):
        meta = get_json("fluview_meta")
        latest = int(meta["epidata"][0]["latest_issue"])

        end_date = epiweek_to_date(latest)
        start_ew = mmwr_epiweek(end_date - timedelta(weeks=WEEKS_BACK))

        out = RAW_ROOT / f"ingest_date={ds}"

        write_json(out / "meta.json", meta)
        write_json(out / "ili.json", get_json("fluview", {
            "regions": REGIONS,
            "epiweeks": f"{start_ew}-{latest}",
            "issues": latest,
        }))
        write_json(out / "clinical.json", get_json("fluview_clinical", {
            "regions": REGIONS,
            "epiweeks": f"{start_ew}-{latest}",
            "issues": latest,
        }))

        (out / "_SUCCESS").write_text("")

    ingest()
