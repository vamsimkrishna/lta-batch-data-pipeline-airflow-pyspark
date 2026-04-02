import os
import json
import logging
from datetime import datetime
from typing import Dict, Any

import requests
from dotenv import load_dotenv


# Project root based on current file location
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

LTA_BUS_ARRIVAL_URL = "https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival"
BOON_LAY_BUS_STOP = os.getenv("LTA_BUS_STOP_CODE", "22009")
LTA_ACCOUNT_KEY = os.getenv("LTA_ACCOUNT_KEY")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s"
)


def fetch_bus_arrival_data() -> Dict[str, Any]:
    if not LTA_ACCOUNT_KEY:
        raise ValueError("LTA_ACCOUNT_KEY is missing.")

    headers = {
        "AccountKey": LTA_ACCOUNT_KEY,
        "accept": "application/json",
    }
    params = {
        "BusStopCode": BOON_LAY_BUS_STOP,
    }

    last_error = None
    for attempt in range(1, 4):
        try:
            logging.info(
                f"Calling LTA BusArrival API for stop {BOON_LAY_BUS_STOP} (attempt {attempt})..."
            )
            response = requests.get(
                LTA_BUS_ARRIVAL_URL,
                headers=headers,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            logging.warning(f"API call failed on attempt {attempt}: {exc}")

    raise RuntimeError(f"Failed to fetch LTA BusArrival data after 3 attempts: {last_error}")


def write_raw_json(data: Dict[str, Any]) -> str:
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    ts = datetime.now()
    filename = f"{ts:%Y-%m-%d_%H-%M-%S}.json"
    filepath = os.path.join(raw_dir, filename)

    enriched_data = {
        "ingestion_timestamp": ts.isoformat(timespec="seconds"),
        "bus_stop_code": BOON_LAY_BUS_STOP,
        "data": data,
    }

    logging.info(f"Writing raw JSON to: {filepath}")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(enriched_data, f, indent=2, ensure_ascii=False)

    logging.info(f"Saved raw data to {filepath}")
    return filepath


def main() -> None:
    logging.info(f"Starting LTA Bus Arrival fetch for Boon Lay Interchange ({BOON_LAY_BUS_STOP})")
    data = fetch_bus_arrival_data()
    output_file = write_raw_json(data)
    logging.info(f"Fetch completed successfully. Output file: {output_file}")


if __name__ == "__main__":
    main()