import os
import glob
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
POLLING_INTERVAL_MINUTES = 2.0


def build_spark() -> SparkSession:
    import sys

    python_path = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    return (
        SparkSession.builder
        .appName("LTABoonLayDailyETL")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .getOrCreate()
    )


def parse_snapshot_timestamp_from_filename(file_path: str) -> Optional[datetime]:
    filename = os.path.basename(file_path)
    stem = os.path.splitext(filename)[0]
    try:
        return datetime.strptime(stem, "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        logging.warning(f"Could not parse timestamp from filename: {filename}")
        return None


def parse_ingestion_timestamp(raw: Dict[str, Any]) -> Optional[datetime]:
    ts = raw.get("ingestion_timestamp")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        logging.warning(f"Could not parse ingestion_timestamp: {ts}")
        return None


def derive_date(raw: Dict[str, Any], file_path: str) -> str:
    ingestion_dt = parse_ingestion_timestamp(raw)
    if ingestion_dt:
        return ingestion_dt.strftime("%Y-%m-%d")

    snapshot_dt = parse_snapshot_timestamp_from_filename(file_path)
    if snapshot_dt:
        return snapshot_dt.strftime("%Y-%m-%d")

    return datetime.now().strftime("%Y-%m-%d")


def get_today_json_files(raw_json_dir: str) -> List[str]:
    today_str = datetime.now().strftime("%Y-%m-%d")
    pattern = os.path.join(raw_json_dir, "*.json")
    all_files = glob.glob(pattern)

    today_files = [
        f for f in all_files
        if os.path.basename(f).startswith(today_str)
    ]
    today_files = sorted(today_files, key=os.path.getmtime)

    if not today_files:
        logging.warning(f"No JSON files found for today in {raw_json_dir}")

    return today_files


def unpack_services(
    raw: Dict[str, Any],
    record_date: str,
    fetch_timestamp: str,
    source_file: str,
) -> List[Dict[str, Any]]:
    payload = raw.get("data", {})
    services = payload.get("Services", [])

    records: List[Dict[str, Any]] = []

    for service in services:
        svc = service.get("ServiceNo")
        op = service.get("Operator")

        for bus_key, bus_rank in [("NextBus", 1), ("NextBus2", 2), ("NextBus3", 3)]:
            bus = service.get(bus_key)
            if not bus or not bus.get("EstimatedArrival"):
                continue

            records.append(
                {
                    "date": record_date,
                    "fetch_timestamp": fetch_timestamp,
                    "source_file": source_file,
                    "service_no": svc,
                    "operator": op,
                    "bus_rank": bus_rank,
                    "estimated_arrival_str": bus.get("EstimatedArrival"),
                    "load": bus.get("Load"),
                    "monitored": bus.get("Monitored"),
                    "latitude": bus.get("Latitude"),
                    "longitude": bus.get("Longitude"),
                }
            )

    return records


def load_daily_records(raw_json_dir: str) -> List[Dict[str, Any]]:
    json_files = get_today_json_files(raw_json_dir)
    if not json_files:
        return []

    all_records: List[Dict[str, Any]] = []

    for raw_json_path in json_files:
        logging.info(f"Reading JSON from: {raw_json_path}")

        with open(raw_json_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        ingestion_dt = parse_ingestion_timestamp(raw)
        if ingestion_dt:
            fetch_timestamp = ingestion_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            snapshot_dt = parse_snapshot_timestamp_from_filename(raw_json_path)
            if snapshot_dt:
                fetch_timestamp = snapshot_dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                fetch_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        record_date = derive_date(raw, raw_json_path)
        source_file = os.path.basename(raw_json_path)

        records = unpack_services(
            raw=raw,
            record_date=record_date,
            fetch_timestamp=fetch_timestamp,
            source_file=source_file,
        )
        all_records.extend(records)

    logging.info(f"Loaded {len(all_records)} raw bus records from {len(json_files)} file(s).")
    return all_records


def prepare_snapshot_level_dataframe(spark: SparkSession, raw_json_dir: str) -> Optional[DataFrame]:
    all_records = load_daily_records(raw_json_dir)
    if not all_records:
        logging.info("No usable records found for today. Exiting cleanly.")
        return None

    bus_df = spark.createDataFrame(all_records)

    bus_df = bus_df.dropDuplicates(
        ["fetch_timestamp", "service_no", "operator", "bus_rank"]
    )

    bus_df = bus_df.withColumn(
        "fetch_timestamp_ts",
        F.to_timestamp(F.col("fetch_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    bus_df = bus_df.withColumn(
        "estimated_arrival_ts",
        F.to_timestamp(
            F.regexp_replace(F.col("estimated_arrival_str"), "T|Z", " "),
            "yyyy-MM-dd HH:mm:ss.SSS",
        ),
    )

    bus_df = bus_df.withColumn(
        "minutes_to_arrival",
        F.round(
            (F.col("estimated_arrival_ts").cast("long") - F.col("fetch_timestamp_ts").cast("long")) / 60.0,
            2,
        )
    )

    bus_df = bus_df.withColumn(
        "load_category",
        F.when(F.col("load") == "SEA", "light")
         .when(F.col("load") == "SDA", "medium")
         .when(F.col("load") == "LSD", "heavy")
         .otherwise("unknown")
    )

    snapshot_df = (
        bus_df.groupBy("date", "fetch_timestamp", "fetch_timestamp_ts", "source_file", "service_no", "operator")
        .agg(
            F.max(F.when(F.col("bus_rank") == 1, F.col("minutes_to_arrival"))).alias("bus1_eta"),
            F.max(F.when(F.col("bus_rank") == 2, F.col("minutes_to_arrival"))).alias("bus2_eta"),
            F.max(F.when(F.col("bus_rank") == 3, F.col("minutes_to_arrival"))).alias("bus3_eta"),
            F.max(F.when(F.col("bus_rank") == 1, F.col("load_category"))).alias("bus1_load_category"),
            F.max(F.when(F.col("bus_rank") == 2, F.col("load_category"))).alias("bus2_load_category"),
            F.max(F.when(F.col("bus_rank") == 3, F.col("load_category"))).alias("bus3_load_category"),
        )
    )

    return snapshot_df


def apply_rollover_matching(snapshot_df: DataFrame) -> DataFrame:
    service_window = (
        Window.partitionBy("date", "service_no", "operator")
        .orderBy("fetch_timestamp_ts")
    )

    df = snapshot_df.withColumn(
        "prev_fetch_timestamp_ts",
        F.lag("fetch_timestamp_ts").over(service_window)
    ).withColumn(
        "prev_bus1_eta",
        F.lag("bus1_eta").over(service_window)
    ).withColumn(
        "prev_bus2_eta",
        F.lag("bus2_eta").over(service_window)
    ).withColumn(
        "prev_bus3_eta",
        F.lag("bus3_eta").over(service_window)
    )

    df = df.withColumn(
        "elapsed_minutes_since_prev",
        F.when(
            F.col("prev_fetch_timestamp_ts").isNotNull(),
            F.round(
                (F.col("fetch_timestamp_ts").cast("long") - F.col("prev_fetch_timestamp_ts").cast("long")) / 60.0,
                2,
            )
        )
    )

    df = df.withColumn(
        "rollover_shift_1",
        F.when(
            F.col("prev_bus1_eta").isNotNull() &
            F.col("elapsed_minutes_since_prev").isNotNull() &
            (F.col("prev_bus1_eta") <= F.lit(POLLING_INTERVAL_MINUTES)),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        "matched_prev_for_bus1",
        F.when(F.col("rollover_shift_1") == 1, F.col("prev_bus2_eta"))
         .otherwise(F.col("prev_bus1_eta"))
    )

    df = df.withColumn(
        "matched_prev_for_bus2",
        F.when(F.col("rollover_shift_1") == 1, F.col("prev_bus3_eta"))
         .otherwise(F.col("prev_bus2_eta"))
    )

    df = df.withColumn(
        "expected_bus1_eta",
        F.when(
            F.col("matched_prev_for_bus1").isNotNull() & F.col("elapsed_minutes_since_prev").isNotNull(),
            F.round(F.col("matched_prev_for_bus1") - F.col("elapsed_minutes_since_prev"), 2)
        )
    )

    df = df.withColumn(
        "expected_bus2_eta",
        F.when(
            F.col("matched_prev_for_bus2").isNotNull() & F.col("elapsed_minutes_since_prev").isNotNull(),
            F.round(F.col("matched_prev_for_bus2") - F.col("elapsed_minutes_since_prev"), 2)
        )
    )

    df = df.withColumn(
        "bus1_eta_drift",
        F.when(
            F.col("expected_bus1_eta").isNotNull() & F.col("bus1_eta").isNotNull(),
            F.round(F.col("bus1_eta") - F.col("expected_bus1_eta"), 2)
        )
    )

    df = df.withColumn(
        "bus2_eta_drift",
        F.when(
            F.col("expected_bus2_eta").isNotNull() & F.col("bus2_eta").isNotNull(),
            F.round(F.col("bus2_eta") - F.col("expected_bus2_eta"), 2)
        )
    )

    df = df.withColumn(
        "bus1_eta_drift_capped",
        F.when(F.col("bus1_eta_drift").isNull(), None)
         .when(F.col("bus1_eta_drift") > 10, 10.0)
         .when(F.col("bus1_eta_drift") < -10, -10.0)
         .otherwise(F.col("bus1_eta_drift"))
    )

    df = df.withColumn(
        "bus2_eta_drift_capped",
        F.when(F.col("bus2_eta_drift").isNull(), None)
         .when(F.col("bus2_eta_drift") > 10, 10.0)
         .when(F.col("bus2_eta_drift") < -10, -10.0)
         .otherwise(F.col("bus2_eta_drift"))
    )

    df = df.withColumn(
        "bus1_delay_minutes",
        F.when(F.col("bus1_eta_drift_capped").isNull(), 0.0)
         .when(F.col("bus1_eta_drift_capped") > 0, F.col("bus1_eta_drift_capped"))
         .otherwise(0.0)
    )

    df = df.withColumn(
        "bus2_delay_minutes",
        F.when(F.col("bus2_eta_drift_capped").isNull(), 0.0)
         .when(F.col("bus2_eta_drift_capped") > 0, F.col("bus2_eta_drift_capped"))
         .otherwise(0.0)
    )

    df = df.withColumn(
        "bus1_on_time_flag",
        F.when(F.col("expected_bus1_eta").isNull(), 1.0)
         .when(F.abs(F.col("bus1_eta_drift_capped")) <= 2.0, 1.0)
         .otherwise(0.0)
    )

    df = df.withColumn(
        "bus2_on_time_flag",
        F.when(F.col("expected_bus2_eta").isNull(), 1.0)
         .when(F.abs(F.col("bus2_eta_drift_capped")) <= 2.0, 1.0)
         .otherwise(0.0)
    )

    return df


def build_daily_kpis(matched_df: DataFrame) -> DataFrame:
    daily_kpis = (
        matched_df
        .groupBy("date", "service_no", "operator")
        .agg(
            F.countDistinct("source_file").alias("num_snapshots"),
            F.round(F.avg("bus1_eta"), 2).alias("avg_bus1_eta_minutes"),
            F.round(F.avg("bus2_eta"), 2).alias("avg_bus2_eta_minutes"),
            F.round(F.avg("bus1_delay_minutes"), 2).alias("avg_bus1_delay_minutes"),
            F.round(F.avg("bus2_delay_minutes"), 2).alias("avg_bus2_delay_minutes"),
            F.round(
                (F.avg("bus1_delay_minutes") + F.avg("bus2_delay_minutes")) / 2,
                2,
            ).alias("avg_delay_minutes"),
            F.round(
                (F.avg("bus1_on_time_flag") + F.avg("bus2_on_time_flag")) / 2 * 100,
                2,
            ).alias("pct_on_time"),
            F.round(F.avg("bus1_eta_drift_capped"), 2).alias("avg_bus1_eta_drift"),
            F.round(F.avg("bus2_eta_drift_capped"), 2).alias("avg_bus2_eta_drift"),
            F.round(
                (
                    F.avg(F.when(F.col("bus1_load_category") == "heavy", 1.0).otherwise(0.0)) +
                    F.avg(F.when(F.col("bus2_load_category") == "heavy", 1.0).otherwise(0.0)) +
                    F.avg(F.when(F.col("bus3_load_category") == "heavy", 1.0).otherwise(0.0))
                ) / 3 * 100,
                2,
            ).alias("pct_heavy_load"),
            F.round(
                (
                    F.avg(F.when(F.col("bus1_load_category") == "medium", 1.0).otherwise(0.0)) +
                    F.avg(F.when(F.col("bus2_load_category") == "medium", 1.0).otherwise(0.0)) +
                    F.avg(F.when(F.col("bus3_load_category") == "medium", 1.0).otherwise(0.0))
                ) / 3 * 100,
                2,
            ).alias("pct_medium_load"),
            F.when(
                (
                    F.max(F.when(F.col("bus1_load_category") == "heavy", 1).otherwise(0)) +
                    F.max(F.when(F.col("bus2_load_category") == "heavy", 1).otherwise(0)) +
                    F.max(F.when(F.col("bus3_load_category") == "heavy", 1).otherwise(0))
                ) > 0,
                "high",
            )
            .when(
                (
                    F.max(F.when(F.col("bus1_load_category") == "medium", 1).otherwise(0)) +
                    F.max(F.when(F.col("bus2_load_category") == "medium", 1).otherwise(0)) +
                    F.max(F.when(F.col("bus3_load_category") == "medium", 1).otherwise(0))
                ) > 0,
                "medium",
            )
            .otherwise("low")
            .alias("peak_crowding_level"),
        )
        .orderBy("date", "service_no")
    )

    return daily_kpis


def transform_bus_records(spark: SparkSession, raw_json_dir: str) -> Optional[DataFrame]:
    snapshot_df = prepare_snapshot_level_dataframe(spark, raw_json_dir)
    if snapshot_df is None:
        return None

    matched_df = apply_rollover_matching(snapshot_df)
    daily_kpis = build_daily_kpis(matched_df)
    return daily_kpis


def save_daily_kpis(daily_kpis: DataFrame, output_kpis_path: str) -> str:
    os.makedirs(output_kpis_path, exist_ok=True)

    csv_file_path = os.path.join(output_kpis_path, "daily_kpis.csv")
    logging.info(f"Writing KPIs to single CSV file via Pandas at: {csv_file_path}")

    daily_kpis_pd = daily_kpis.toPandas()
    daily_kpis_pd.to_csv(csv_file_path, index=False)

    logging.info(f"CSV successfully written: {csv_file_path}")
    return csv_file_path


if __name__ == "__main__":
    spark = build_spark()
    try:
        raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
        out_dir = os.path.join(PROJECT_ROOT, "data", "processed", "kpis")

        kpis = transform_bus_records(spark, raw_dir)

        if kpis is None:
            logging.info("No KPI output generated. Exiting cleanly.")
        else:
            output_file = save_daily_kpis(kpis, out_dir)
            logging.info("Preview of KPI output:")
            kpis.show(20, truncate=False)
            logging.info(f"Done. Output file: {output_file}")
    finally:
        spark.stop()