# Swedish Public Transport Analytics Pipeline

An end-to-end data platform that ingests **live** Swedish public-transport feeds, models actual-vs-scheduled performance, and surfaces chronic delay patterns on a dashboard. Built around the **Uppsala / Upplands Lokaltrafik (UL) bus network** using [Trafiklab](https://trafiklab.se) open data.

> Streaming ingestion → data lake → distributed transform → warehouse → tested models → BI dashboard, fully orchestrated and containerised.

---

## Architecture

```
            ┌──────────────────── Apache Airflow (Dockerised, scheduled DAG) ────────────────────┐
            │                                                                                     │
GTFS-RT ───►│  producer.py ──► Kafka ──► consumer.py ──► GCS (raw) ──► PySpark ──► BigQuery        │──► dbt ──► Looker
(TripUpdates│  (whole feed)   (KRaft)   (1 file/pull)   trafiklab-    (parse·flatten   staging     │  (staging  Studio
 UL buses)  │                                          raw-data       ·join·load)      dataset     │  + marts   (4 panels)
            └─────────────────────────────────────────────────────────────────────────────────────┘  + tests)
GTFS Sweden 3 static (stops · trips · routes) ───────────────────────────────► joined in the PySpark step
```

**Flow:** a Kafka producer pulls the live GTFS-Realtime `TripUpdates` feed and forwards each snapshot to Kafka; a consumer lands it in Google Cloud Storage as the raw layer; a PySpark job parses the protobuf, flattens it, joins the static GTFS schedule for human-readable labels, and loads BigQuery; dbt models that raw table into clean, tested marts; Looker Studio reads the marts. Airflow runs producer → consumer → transform on a schedule.

---

## Tech stack

| Layer | Tool |
|---|---|
| Streaming | **Apache Kafka** 4.3 (KRaft, no Zookeeper) |
| Containerisation | **Docker / Docker Compose** |
| Data lake | **Google Cloud Storage** |
| Orchestration | **Apache Airflow** 3.2 (custom image with Java + PySpark) |
| Distributed transform | **PySpark** 4.1 |
| Warehouse | **BigQuery** |
| Transformation / modelling | **dbt** (dbt-bigquery) |
| BI | **Data Studio (Looker Studio)** |
| Data | Trafiklab GTFS-RT + GTFS Sweden 3 static (protobuf + GTFS CSV) |

---

## Data model (dbt)

Source → staging → marts, each layer with a clear job:

- **`stg_realtime_delays`** (view) — cleans/renames/types the raw table; derives `arrival_ts` (`TIMESTAMP_SECONDS`) and `delay_minutes`; flags `has_valid_arrival_time`. Keeps **all** rows (faithful base; filtering is a downstream choice).
- **`mart_line_performance`** — one row per line: `avg_delay_minutes`, `pct_on_time`, `stop_events`.
- **`mart_line_hourly`** — one row per line × local hour-of-day (for the heatmap).
- **`mart_chronic_delays`** — one row per line with an `is_chronic` flag: lines whose on-time % is **> 2σ below the network average** (statistical anomaly queue).
- **`mart_pipeline_health`** — single-row operational snapshot: data freshness + volume.

**On-time** is defined as an *asymmetric* window (1 min early → 3 min late), tighter on early because an early bus makes riders miss it. The thresholds are **dbt variables**, so the KPI is defined in one place. Tests cover non-null keys and uniqueness of each mart's grain.

---

## Dashboard

Four Looker Studio panels off the marts:
1. **On-time % by line** (bar)
2. **Delay by line × time-of-day** (heatmap)
3. **Chronic underperformers** (filtered table)
4. **Pipeline health** (freshness / volume scorecards)

*(Add a screenshot here.)*

---

## Key engineering decisions & lessons

- **Cross-container Kafka networking.** Kafka and Airflow ran as separate Compose projects on isolated networks — `localhost:9092` from inside Airflow meant the Airflow container, not Kafka. Solved with a **shared Docker network**, **dual Kafka listeners** (host `localhost:9092` + internal `container-name:29092`), and **environment-driven config** so the same code runs on host and in-container.
- **Small-files anti-pattern.** The producer originally serialised one entity per Kafka message → thousands of tiny GCS files (bad for Spark). Refactored the producer to forward the **whole feed snapshot per pull**, cutting file count drastically.
- **ELT layering.** Ingestion lands **raw, faithful** data; all derivations (timestamps, minutes, on-time logic, filters) live in **dbt** — one source of truth, cheap to change, and the granular layer stays available for analysts/DS while marts serve the dashboard.
- **Feed product-family pairing (a real Trafiklab gotcha).** A join returned 100% nulls because the static feed (`GTFS Regional`) didn't share stop IDs with the realtime feed (`GTFS Sweden 3`). Fix: pair feeds from the **same product family**. Also handled the Swedish hierarchical stop-ID scheme (`9021…` StopArea vs `9022…` StopPoint).
- **Join debugging.** Diagnosed nulls down to a **type mismatch** (protobuf IDs are strings; CSV inference made them `INT64`) and a **data-coverage** mismatch — *trust, but verify your keys*.
- **Timezones & DST.** Timestamps stored in **UTC**, converted to local with the **named zone** `Europe/Stockholm` so daylight-saving is handled automatically (never a fixed offset).
- **Statistical anomaly detection.** Chronic underperformers flagged at **> 2σ** from the network mean, with a minimum sample-size guard — a deliberately high bar for few, high-confidence flags.

---

## Known limitations / future work

- **Deduplication.** GTFS-RT re-reports each active trip every pull, so the same `(trip, stop)` appears across snapshots with *evolving predictions*. Metrics currently include these re-reports (directionally correct, not exact). Proper fix: capture the feed **observation timestamp** and keep one record per `(trip, stop)` (latest) in an intermediate model.
- **Schedule reliability.** GTFS times at `timepoint=0` stops are *interpolated estimates*, so delays measured there are less reliable than at `timepoint=1` (committed) stops — a polished analysis would weight or filter on this.
- **Distributed parsing.** Protobuf is parsed driver-side in the Spark job (fine at this volume); the scalable version would distribute parsing across executors.
- **Spark execution.** Spark runs *inside* the Airflow worker (fine for local/portfolio); production-standard is to **submit** to a dedicated cluster (Dataproc / `SparkSubmitOperator`).
- **Warehouse scaling.** At larger volumes, **partition/cluster** the staging table by date to cut bytes scanned.
- **CI/CD.** A GitHub Actions workflow running `dbt build` on push is planned.
- **Time-window scoping.** Marts aggregate all loaded data; a rolling-window (e.g. 7-day) scope would sharpen the dashboard.

---

## Repository structure

```
trafiklab-pipeline/
├── docker-compose.yml        # Kafka (KRaft)
├── kafka/                    # producer.py, consumer.py
├── ingestion/                # static GTFS download
├── airflow/                  # Dockerfile (Java+PySpark), docker-compose, dags/
├── pyspark/                  # transform_realtime.py (GCS → parse → join → BigQuery)
└── trafiklab_dbt/            # dbt project: staging + marts + tests
```

## Running locally (overview)

1. Register at [trafiklab.se](https://trafiklab.se) for **GTFS Sweden 3 Realtime** + **GTFS Sweden 3 Static** keys; put them in `.env` (gitignored).
2. A GCP project with a GCS bucket + BigQuery dataset, and a service-account key (gitignored).
3. `docker compose up -d` (Kafka), then `docker compose up -d` in `airflow/` (build the custom image first).
4. Trigger the `trafiklab_realtime_dag` to ingest + transform into BigQuery.
5. `cd trafiklab_dbt && dbt build` to model + test.
6. Connect Looker Studio to the marts.

---

*Built as a hands-on data-engineering portfolio project — from a live protobuf feed to a tested, dashboarded warehouse.*
