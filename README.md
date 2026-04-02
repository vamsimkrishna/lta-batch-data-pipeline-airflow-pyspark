# 🚀 LTA Batch Data Pipeline using Airflow & PySpark

## 📌 Overview

This project builds a **batch data engineering pipeline** using **Apache Airflow and PySpark** to process transport-style data inspired by Singapore LTA systems.

The pipeline automates:
- Data ingestion
- Data transformation
- Data storage for reporting

It simulates a real-world scenario where daily data is processed to generate KPI-ready datasets.

---

## 🎯 Problem Statement

Transport systems generate large volumes of daily data, but raw data is:
- Unstructured
- Inconsistent
- Not directly usable for analysis

This project solves that by building a **scalable ETL pipeline** that:
- Cleans raw data
- Applies transformations
- Produces structured outputs for reporting

---

## 🏗️ Architecture


Raw Data → Airflow DAG → PySpark Processing → Processed Data Output


### Flow:
1. Airflow schedules the pipeline
2. Fetch script loads raw data
3. PySpark transforms the data
4. Output is saved for analytics/KPI reporting

---

## ⚙️ Tech Stack

- **Python**
- **Apache Airflow** (Workflow orchestration)
- **PySpark** (Data processing)
- **Pandas** (Optional lightweight processing)
- **Local File System / Storage**

---

## 📂 Project Structure


project/
│
├── dags/ # Airflow DAG definitions
├── src/ # Core ETL scripts
│ ├── fetch.py
│ ├── transform.py
│ └── load.py
│
├── data/
│ ├── raw/
│ └── processed/
│
├── config/ # Config files / environment settings
├── logs/ # Airflow logs
├── notebooks/ # Analysis / debugging notebooks
└── requirements.txt


---

## 🔄 Pipeline Workflow

### 1️⃣ Fetch Step
- Reads raw data from source
- Validates structure
- Stores in raw folder

### 2️⃣ Transform Step (PySpark)
- Cleans missing/null values
- Applies business logic
- Formats columns
- Aggregates useful metrics

### 3️⃣ Load Step
- Writes processed data
- Saves output for reporting or dashboards

---

## 📊 Example Use Case

- Daily transport data processing
- KPI tracking (delays, counts, anomalies)
- Preparing datasets for Power BI / dashboards

---

## ⏱️ Airflow DAG

- Automatically detected by Airflow
- Runs on schedule (daily/hourly)
- Uses task dependencies:

fetch → transform → load


---

## 🧠 Key Concepts Demonstrated

- ETL pipeline design
- Workflow orchestration using Airflow
- Distributed data processing with PySpark
- Modular code structure
- Production-style folder organization

---

## 🚧 Challenges & Solutions

### Problem 1: Airflow DAG not detecting
**Solution:** Ensured DAG file is placed inside `dags/` folder and syntax is correct.

---

### Problem 2: PySpark setup issues
**Solution:** Installed correct PySpark version and configured environment variables.

---

### Problem 3: Data not passing between tasks
**Solution:** Used XCom to pass metadata (not large data).

---

### Problem 4: File path issues
**Solution:** Used absolute paths and config-based structure.

---

### Problem 5: Dependency errors
**Solution:** Managed dependencies using `requirements.txt`.

---

## 📈 Future Improvements

- AWS S3 integration for storage
- Kafka for real-time streaming
- Data warehouse (Redshift / Snowflake)
- Dashboard integration (Power BI / Tableau)
- Data quality checks (Great Expectations)

---

## 🎤 1-Minute Interview Pitch

This project demonstrates a batch data pipeline built using Airflow and PySpark. It processes raw transport data into structured outputs for analytics. It showcases ETL design, workflow orchestration, and scalable data processing, similar to real-world data engineering systems.

---

## 🧾 How to Run

1. Install dependencies:

pip install -r requirements.txt


2. Start Airflow:

airflow standalone


3. Place DAG in `dags/` folder

4. Trigger DAG from UI

---

## 👨‍💻 Author

Vamsi Krishna  
Data Engineering Enthusiast  
Singapore  

---

## ⭐ Why This Project Matters

This project is designed to:
- Be **interview-ready**
- Showcase **real-world pipeline design**
- Demonstrate **core data engineering skills**
