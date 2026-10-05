# Turbofan Engine Health Monitoring — An Analytics-Engineering Pipeline

An end-to-end pipeline for the NASA C-MAPSS turbofan degradation dataset:
Postgres (warehouse) + dbt (transformation, with tests) + Grafana
(dashboard), orchestrated by Airflow. Built the way an operations
analytics team would actually build it — not as a one-off notebook.

**The question it answers:** given an engine's live sensor readings, is it
healthy, worth watching, or approaching failure — and how much warning
does that actually give a maintenance team?

## Screenshots

!\[Fleet health dashboard](docs/dashboard.png.png)
*Grafana dashboard: per-engine health trend, warning lead time, zone
distribution, and fleet-level headline stats.*

!\[Airflow DAG run](docs/airflow\_dag.png.png)
*Airflow orchestrating extract → dbt run → dbt test, including a
scheduled (unattended) run alongside a manual trigger.*

## Why it's built this way

Most public C-MAPSS projects go straight to an ML regression model
predicting Remaining Useful Life, competing on RMSE against dozens of
published papers on this exact benchmark. This project does something
different: it treats C-MAPSS as if it were a recurring operational data
feed, and builds the pipeline an analytics team would maintain — with
orchestration, tested transformations, and an interpretable health score
instead of a black-box prediction.

## Results (FD001, 100 engines, run-to-failure training data)

|Metric|Value|
|-|-|
|Engines flagged critical before failure|100 / 100|
|Average warning before failure|41.9 cycles|
|Minimum warning before failure|18 cycles|
|Healthy-zone accuracy, >100 cycles from failure|91.1% healthy, 8.9% watch, 0% critical|
|Critical-zone accuracy, last 25 cycles|98.2% critical|

The escalation is monotonic across an engine's life:

|Cycles remaining|Healthy|Watch|Critical|
|-|-|-|-|
|>100|91.1%|8.9%|0%|
|51-100|29.9%|63.6%|6.5%|
|26-50|0.2%|50.3%|49.6%|
|Last 25|0%|1.8%|98.2%|

**On the false-alarm rate:** 8.9% of cycles more than 100 cycles from
failure are flagged "watch." That's not noise to chase to zero — it's the
real trade-off in any early-warning system between catching problems
early and raising some flags that turn out fine. A stricter cutoff would
lower that number but also shrink the warning window on genuine failures.
The thresholds in this build (see below) are a deliberate point on that
trade-off, not an attempt to eliminate it.

**Scope of this result:** validated on the training set the model was
built against — a genuine in-sample check that the indicator responds
correctly to degradation, not an out-of-sample performance guarantee. See
"Honest scope notes" below.

## Architecture

```
Raw sensor files (train\\\_FD001.txt)
        │
        ▼
   Airflow DAG  ──────────────▶  (optional) PySpark, for higher volume
        │
        ▼
   PostgreSQL (raw schema)
        │
        ▼
       dbt  (staging → intermediate → marts, with tests)
        │
        ▼
     Grafana  (fleet health dashboard, provisioned datasource)
```

## Project layout

```
cmapss\\\_pipeline/
├── docker-compose.yml            # Postgres (5433 on host) + Grafana (3000) + Airflow (8080)
├── Dockerfile.airflow            # Airflow image + the load script's/dbt's dependencies
├── requirements-airflow.txt      # packages installed on top of the base Airflow image
├── dbt\\\_profiles/profiles.yml     # dbt connection profile used INSIDE the Airflow container
├── grafana/provisioning/         # pre-configured Postgres datasource
├── init\\\_sql/                     # schema bootstrap, runs on first container boot
├── data/raw/                     # put train\\\_FD001.txt here (not committed)
├── scripts/
│   └── load\\\_raw\\\_data.py          # extract/load step, callable standalone or from Airflow
├── dags/
│   └── cmapss\\\_pipeline\\\_dag.py    # Airflow 3.x DAG: extract\\\_and\\\_load -> dbt\\\_run -> dbt\\\_test
├── dbt/cmapss\\\_dbt/
│   ├── dbt\\\_project.yml           # thresholds are dbt vars — tune without touching SQL
│   ├── profiles\\\_example.yml      # copy to \\\~/.dbt/profiles.yml
│   └── models/
│       ├── staging/              # stg\\\_cmapss\\\_\\\_sensors + source tests
│       ├── intermediate/         # int\\\_engine\\\_health: composite health indicator (z-score based)
│       └── marts/                # fct\\\_engine\\\_health: dashboard-ready, with alert zones
└── requirements.txt
```

## Setup

1. **Get the data.** Download the C-MAPSS dataset (NASA or the Kaggle
mirror) and place `train\\\_FD001.txt` in `data/raw/`.
2. **Python environment:**

```
   python -m venv venv
   venv\\\\Scripts\\\\activate.bat      # Windows cmd.exe
   pip install -r requirements.txt
   ```

3. **Start the warehouse and dashboard:**

```
   docker compose up -d
   ```

This creates the `raw`, `staging`, `intermediate`, and `marts` schemas
automatically, and starts Grafana with the Postgres connection
pre-provisioned. Postgres is published on host port **5433** (not the
default 5432) to avoid clashing with a native Postgres install — set
`CMAPSS\\\_DB\\\_PORT=5433` in every terminal session before running the load
script or dbt.

4. **Load the raw data:**

```
   python scripts\\\\load\\\_raw\\\_data.py --file data\\\\raw\\\\train\\\_FD001.txt --dataset-id FD001
   ```

5. **Configure and run dbt:**

```
   copy dbt\\\\cmapss\\\_dbt\\\\profiles\\\_example.yml "%USERPROFILE%\\\\.dbt\\\\profiles.yml"
   cd dbt\\\\cmapss\\\_dbt
   dbt debug
   dbt run
   dbt test
   ```

6. **Open the dashboard.** http://localhost:3000 (login `admin`/`admin`
on first run). The CMAPSS Warehouse datasource is pre-configured.
Dashboard panels: per-engine health trend, warning lead time per
engine, zone distribution, and two headline stats (engines flagged,
average warning).
7. **Orchestrate with Airflow** (optional — see scope notes). Airflow
doesn't run natively on Windows, so it runs as its own Docker service
instead (see `Dockerfile.airflow`) — this keeps the whole pipeline
runnable with one command regardless of host OS:

```
   docker compose up -d --build
   ```

The `--build` is only needed the first time, or after changing
`Dockerfile.airflow`/`requirements-airflow.txt`. Airflow runs in
"standalone" mode (webserver + scheduler + a SQLite metadata db in one
container) — fine for a portfolio project, not how you'd run this in
production. First boot takes a minute or two; watch for it with:

```
   docker compose logs -f airflow
   ```

Look for a line printing the auto-generated `admin` password, then
open **http://localhost:8080**, log in, and trigger the `cmapss\\\_pipeline`
DAG. It connects to Postgres over Docker's internal network
(`warehouse:5432`), separate from the `localhost:5433` mapping you use
from Windows tools.

## The health indicator — and why it needed a rewrite

**v1 (initial build):** measured each sensor's drift as a percentage of
its raw value. C-MAPSS sensors are large numbers (e.g. \~642) that shift
by only 0.2-1.8% between early life and failure, so the score barely
moved — every engine showed 99%+ "healthy" right up to the moment it
failed. Technically correct, completely uninformative.

**v2 (current):** measures drift in units of each sensor's own
variability (a z-score against the fleet), using the first 10 cycles as
each engine's baseline and a 5-cycle rolling average to reduce noise.
Validated against the raw data first — the six sensors used
(2, 3, 4, 7, 11, 15) each shift by roughly 2.4-2.8 fleet standard
deviations between early life and failure, which is what justifies
including them rather than a longer or shorter list.

This is the most important thing in this project to describe honestly in
an interview: the first version passed its own tests and looked fine
until it was checked against what the dashboard actually showed. Testing
a metric against real output, not just against whether it runs, is what
caught it.

## Alert zone thresholds

Set in `dbt/cmapss\\\_dbt/dbt\\\_project.yml` as dbt vars — change and re-run
`dbt run`, no SQL editing needed:

```yaml
vars:
  baseline\\\_cycles: 10
  healthy\\\_threshold: 0.70   # health >= this -> healthy
  watch\\\_threshold: 0.45     # health >= this -> watch, below -> critical
```

These were validated (not just guessed) against the life-stage breakdown
in the Results section above, and left unchanged after that check —
retuning further against the same 100 engines would risk overfitting the
thresholds to this one dataset.

## Honest scope notes

* **This is an in-sample result.** The thresholds and sensor list were
both developed and validated against the same 100 training engines.
That's a legitimate check that the indicator responds to real
degradation — it is not proof it would generalize to new engines. The
credible next step is loading `test\\\_FD001.txt` and `RUL\\\_FD001.txt`
(provided by NASA, held out from training) and checking whether
truncated test engines are flagged appropriately given their true
remaining life.
* **Spark and a cloud warehouse (Snowflake/BigQuery) are not included in
this base build.** At \~20,000 rows, Postgres and dbt alone are the
right-sized tools — adding Spark here would be performative, not
necessary. The architecture diagram shows where a PySpark step would
slot in if extended to all four FD00x sub-datasets combined, or a
synthetic higher-volume stream — a reasonable extension to build and
document, not a requirement.
* **The Airflow schedule (`@daily`) is illustrative.** C-MAPSS is a
static dataset; the DAG is written as if new data arrived on a
schedule because that's the realistic production pattern, not because
this specific dataset needs it.
* **Scaling assumes one operating condition** (true for FD001/FD003).
FD002/FD004 have six operating conditions and would need sensor values
scaled per-condition before this z-score approach would be valid.
* **`rul\\\_proxy\\\_cycles` is only meaningful on run-to-failure data.** If
you load the official held-out test split, don't present that column
as a real RUL — it reflects only where NASA truncated the file.

