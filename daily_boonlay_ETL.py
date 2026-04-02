import os
import subprocess
from datetime import datetime, time, timedelta

import pendulum
import requests
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator


# Project paths
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_DIR, ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
DAG_ENV_FILE = os.path.join(PROJECT_ROOT, ".env")

FETCH_SCRIPT = os.path.join(SRC_DIR, "fetch_lta_data.py")
TRANSFORM_SCRIPT = os.path.join(SRC_DIR, "transform_bus_data_v1.py")

# Load environment variables
load_dotenv(DAG_ENV_FILE)

TZ = pendulum.timezone("Asia/Singapore")

LTA_ACCOUNT_KEY = os.getenv("LTA_ACCOUNT_KEY")
BOON_LAY_BUS_STOP = os.getenv("LTA_BUS_STOP_CODE", "22009")
BUS_ROUTES_URL = "https://datamall2.mytransport.sg/ltaodataservice/BusRoutes"

PRE_START_BUFFER_MIN = 5
POST_END_BUFFER_MIN = 20

default_args = {
    "owner": "vamsi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _hhmm_to_time(value: str):
    if not value:
        return None

    value = str(value).strip()
    if len(value) != 4 or not value.isdigit():
        return None

    hh = int(value[:2])
    mm = int(value[2:])

    if hh > 23 or mm > 59:
        return None

    return time(hh, mm)


def _service_day_columns(now_local: datetime):
    weekday = now_local.weekday()

    if weekday == 5:
        return "SAT_FirstBus", "SAT_LastBus"
    if weekday == 6:
        return "SUN_FirstBus", "SUN_LastBus"
    return "WD_FirstBus", "WD_LastBus"


def _fetch_all_bus_routes():
    if not LTA_ACCOUNT_KEY:
        raise ValueError("LTA_ACCOUNT_KEY is missing from .env or environment.")

    headers = {
        "AccountKey": LTA_ACCOUNT_KEY,
        "accept": "application/json",
    }

    results = []
    skip = 0

    while True:
        resp = requests.get(
            BUS_ROUTES_URL,
            headers=headers,
            params={"$skip": skip},
            timeout=60,
        )
        resp.raise_for_status()

        batch = resp.json().get("value", [])

        if not batch:
            break

        results.extend(batch)
        skip += len(batch)

        if len(batch) < 500:
            break

    return results


def _compute_boon_lay_active_window(now_local: datetime):
    first_col, last_col = _service_day_columns(now_local)

    rows = _fetch_all_bus_routes()
    stop_rows = [r for r in rows if str(r.get("BusStopCode")) == BOON_LAY_BUS_STOP]

    if not stop_rows:
        return None, None, 0

    first_times = []
    last_times = []

    for row in stop_rows:
        first_bus = _hhmm_to_time(row.get(first_col))
        last_bus = _hhmm_to_time(row.get(last_col))

        if first_bus:
            first_times.append(first_bus)
        if last_bus:
            last_times.append(last_bus)

    if not first_times or not last_times:
        return None, None, len(stop_rows)

    start_dt = datetime.combine(now_local.date(), min(first_times))
    end_dt = datetime.combine(now_local.date(), max(last_times))

    return start_dt, end_dt, len(stop_rows)


def should_run_pipeline(**context):
    now_local = pendulum.now(TZ).naive()

    start_dt, end_dt, count = _compute_boon_lay_active_window(now_local)

    if not start_dt or not end_dt:
        print("Skipping: cannot determine service window")
        return False

    start_dt -= timedelta(minutes=PRE_START_BUFFER_MIN)
    end_dt += timedelta(minutes=POST_END_BUFFER_MIN)

    print(f"Services at stop {BOON_LAY_BUS_STOP}: {count}")
    print(f"Window: {start_dt} -> {end_dt}")
    print(f"Now: {now_local}")

    return start_dt <= now_local <= end_dt


def run_fetch():
    result = subprocess.run(
        ["python", FETCH_SCRIPT],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"Fetch failed:\n{result.stderr}")


def run_transform():
    result = subprocess.run(
        ["python", TRANSFORM_SCRIPT],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"Transform failed:\n{result.stderr}")


with DAG(
    dag_id="daily_boon_lay_etl",
    default_args=default_args,
    description="LTA Boon Lay pipeline",
    start_date=pendulum.datetime(2026, 3, 29, 5, 0, tz=TZ),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lta", "bus", "pyspark"],
) as dag:

    check_service_window = ShortCircuitOperator(
        task_id="check_service_window",
        python_callable=should_run_pipeline,
    )

    fetch = PythonOperator(
        task_id="fetch_lta_data",
        python_callable=run_fetch,
    )

    transform = PythonOperator(
        task_id="transform_bus_data",
        python_callable=run_transform,
    )

    check_service_window >> fetch >> transform