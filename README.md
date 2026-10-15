# Freddie Mac Mortgage Credit Risk & Portfolio Monitoring

An end-to-end mortgage credit-risk analytics workflow on Freddie Mac's
[Single-Family Loan-Level Dataset](https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset) (Release 47):

- **13.6 million loans**, 24 origination vintages (2018Q1–2023Q4)
- **627 million monthly performance records**, observed through March 2026
- Spark ETL → Parquet → analytical risk marts → PostgreSQL → Tableau dashboards

It covers origination risk segmentation, fixed-horizon delinquency outcomes, portfolio delinquency and UPB exposure
monitoring, delinquency roll-forward and cure dynamics, and loan lifecycle / terminal outcomes.

```
Freddie Mac yearly ZIPs
        │  00 unzip (year ZIP → quarter ZIP → orig/perf TXT)
        ▼
data/raw_txt/{year}/{Q}/
        │  01–02 Spark ETL (schema-driven, typed, vintage-partitioned)
        ▼
data/processed/orig, perf                     (Parquet)
        │  03–05 risk marts
        ▼
risk_analysis · portfolio_monthly · delinquency_transition · loan_lifecycle
        │  06 JDBC load + SQL validation
        ▼
PostgreSQL (schema: risk)  ──▶  Tableau dashboards
```

## Analytical marts

| Mart | Grain | Contents |
|---|---|---|
| `risk.risk_analysis` | one row per loan | Origination characteristics (FICO, LTV, CLTV, DTI, UPB, rate, purpose, occupancy, …) + 12M / 24M ever 30/60/90+ DPD outcomes |
| `risk.portfolio_monthly` | vintage × reporting month | Active loans and UPB; Current / 30 / 60 / 90+ DPD counts and exposures; prepayments |
| `risk.delinquency_transition` | vintage × month × from-state × to-state | Month-over-month migration between Current / 30 / 60 / 90+ DPD / Exit, by loan count and UPB |
| `risk.loan_lifecycle` | one row per loan | First 30/60/90+ DPD periods, max delinquency, lifetime flags, terminal status, last observed balance |

`risk.transition_rates` (view) fills in zero-count transitions so roll and cure rates can be computed for every
from-state.

## Dashboards

Built in Tableau on top of the PostgreSQL marts (PDF exports in [`dashboards/`](dashboards/)):

| Dashboard | Key views |
|---|---|
| [Origination Risk & Credit Segmentation](dashboards/Origination%20Risk%20%26%20Credit%20Segmentation.pdf) | FICO and LTV risk segmentation, vintage risk trends, FICO × LTV heatmap |
| [Portfolio Delinquency & Exposure Monitoring](dashboards/Portfolio%20Delinquency%20%26%20Exposure%20Monitoring.pdf) | Monthly 30 / 60 / 90+ DPD trends, 90+ loan rate vs UPB share, active UPB, 90+ UPB concentration by vintage |
| [Delinquency Migration & Recovery Dynamics](dashboards/Delinquency%20Migration%20%26%20Recovery%20Dynamics.pdf) | Roll-forward rates, cure rates, monthly migration matrix, loan-count vs UPB-weighted 60 → 90+ roll rate |
| [Loan Lifecycle & Terminal Outcomes](dashboards/Loan%20Lifecycle%20%26%20Terminal%20Outcomes.pdf) | Lifecycle status by vintage, terminal outcome mix, max delinquency distribution, time to first 90+ DPD |

### Key findings

- Borrower credit quality has a clear monotonic relationship with serious delinquency: the **24-month 90+ DPD rate
  for FICO < 660 is roughly 10× the rate for FICO 780+**.
- Loan-count delinquency and dollar exposure tell different stories; the portfolio dashboard tracks both.
- Most terminal records are prepayment / maturity rather than credit events, so **zero balance should not be treated
  as default**.

## Data validation

The marts are reconciled against each other on the full dataset
([`sql/validate_risk_marts.sql`](sql/validate_risk_marts.sql)):

- `risk_analysis` and `loan_lifecycle` both contain **13,589,191 loans**, with 0 missing in either direction
- Portfolio and lifecycle loan-month totals reconcile exactly (**627,341,737**)
- 0 delinquency-bucket reconciliation errors, 0 lifecycle hierarchy errors, 0 terminal-status inconsistencies
- Only 40 non-consecutive monthly gaps across more than 613 million potential transitions

The same checks run in Spark for a single vintage with
[`scripts/validate_perf_marts_one_vintage.py`](scripts/validate_perf_marts_one_vintage.py).

## Analytical considerations

- Vintages have different observation windows, so lifetime metrics are not compared across vintages directly;
  **fixed 12M / 24M outcomes** are used instead.
- Calendar-month portfolio monitoring is distinct from months-on-book vintage analysis.
- Terminal status does not imply credit default (most zero-balance events are prepayment or maturity).
- Fixed-horizon outcomes and time-to-90+ DPD are measured from the **first observed performance month**, not the
  origination date.

## How to run

### 1. Get the data

The Freddie Mac dataset requires a free account and acceptance of Freddie Mac's terms of use, which do not allow
redistribution, so no data is included in this repo.

1. Register at the [Single-Family Loan-Level Dataset](https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset)
   page and download the **Standard Dataset** yearly files `historical_data_2018.zip` … `historical_data_2023.zip`.
2. Put them in `data/raw_zip/`.

Disk: ~9 GB of ZIPs, plus the unzipped TXT and ~8 GB of Parquet.

### 2. Environment

Python 3.11, **Java 17** (for Spark), PostgreSQL 17 for the database step.

```bash
conda create -n freddie python=3.11 openjdk=17 -y
conda activate freddie
pip install -r requirements.txt
```

### 3. Spark pipeline

```bash
./run_pipeline.sh
```

This runs steps 00–05. Each step writes per-vintage partitions and skips vintages that already have a `_SUCCESS`
marker, so an interrupted run can simply be restarted. To run a single step, call it from the project root:

```bash
PYTHONPATH=. python scripts/03_build_targets.py
```

### 4. Load into PostgreSQL and validate

```bash
bash scripts/setup_postgres.sh   # installs PostgreSQL via Homebrew, creates user, database, and schema
psql -h localhost -U freddie_user -d freddie_mac_risk -f sql/create_risk_schema.sql
PYTHONPATH=. python scripts/06_load_to_postgres.py   # prompts for the database password
psql -h localhost -U freddie_user -d freddie_mac_risk -f sql/create_view.sql
psql -h localhost -U freddie_user -d freddie_mac_risk -f sql/validate_risk_marts.sql
```

## Project structure

```
freddie-mac-credit-risk/
├── run_pipeline.sh                      # steps 00–05 in order
├── requirements.txt
├── config/schemas/
│   └── freddie_mac_release47.json       # column names, positions, and types for orig (31) and perf (35)
├── scripts/
│   ├── 00_unzip_raw_data.sh             # nested year → quarter ZIP extraction
│   ├── 01_etl_orig.py                   # origination TXT → Parquet
│   ├── 02_etl_perf.py                   # performance TXT → Parquet
│   ├── 03_build_targets.py              # 12M / 24M delinquency outcomes per loan
│   ├── 04_build_risk_dataset.py         # origination ⨝ targets → risk_analysis
│   ├── 05_build_perf_marts.py           # portfolio_monthly, delinquency_transition, loan_lifecycle
│   ├── 06_load_to_postgres.py           # JDBC load of all marts
│   ├── setup_postgres.sh
│   └── validate_perf_marts_one_vintage.py
├── src/
│   ├── utils.py                         # Spark session, schema loading/validation, TXT → Parquet
│   ├── portfolio_monthly.py
│   ├── delinquency_transition.py
│   └── loan_lifecycle.py
├── sql/
│   ├── create_risk_schema.sql
│   ├── create_view.sql
│   └── validate_risk_marts.sql
├── dashboards/                          # Tableau PDF exports
└── docs/
    └── column_dictionary.md             # Release 47 column reference (Chinese descriptions)
```

## Roadmap

Planned next phases, building on the `risk_analysis` mart:

- **Credit risk model** — predict `ever_90dpd_24m` from origination attributes: logistic regression baseline and a
  gradient-boosting challenger, evaluated on ROC-AUC, KS, calibration, and bad rate by score band.
- **Underwriting strategy simulation** — turn scores into risk tiers (approve / monitor / manual review / decline) and
  compare approval rate, bad rate, risk capture, and review volume across thresholds.
- **Model validation** — out-of-time testing by vintage, calibration, population stability (PSI), and segment-level
  performance.
- **Monitoring & stress scenarios** — baseline / moderate / severe scenarios on roll and cure rates to project
  delinquency and exposure.

## Tech stack

PySpark · Spark SQL · Parquet · PostgreSQL · SQL · Tableau · Bash
