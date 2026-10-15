from pathlib import Path

from src.utils import ensure_folder_exists, create_spark_session

from src.portfolio_monthly import (
    build_portfolio_monthly,
)

# 等后面两个 function 写好以后直接打开
from src.delinquency_transition import (
    build_delinquency_transition,
)

from src.loan_lifecycle import (
    build_loan_lifecycle,
)

from pyspark.sql import functions as F


def validate_perf_marts(
    perf,
    portfolio,
    transition,
    lifecycle,
):

    print("\n" + "=" * 70)
    print("VALIDATING PERF MARTS")
    print("=" * 70)

    errors = []


    # ========================================================
    # 1. Basic source counts
    # ========================================================

    perf_count = perf.count()

    perf_loan_count = (
        perf
        .select("loan_identifier")
        .distinct()
        .count()
    )

    print(f"Perf rows:        {perf_count:,}")
    print(f"Distinct loans:   {perf_loan_count:,}")


    # ========================================================
    # 2. Portfolio Monthly
    # ========================================================

    print("\n[1] Portfolio Monthly")


    # One row per period
    duplicate_periods = (
        portfolio
        .groupBy("period")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_periods > 0:
        errors.append(
            "Portfolio: duplicate periods found."
        )


    # Active <= Observed
    invalid_active = (
        portfolio
        .filter(
            F.col("active_loan_count")
            > F.col("observed_loan_count")
        )
        .count()
    )

    if invalid_active > 0:
        errors.append(
            "Portfolio: active count exceeds observed count."
        )


    # Active should reconcile to delinquency buckets
    invalid_reconciliation = (
        portfolio
        .filter(
            F.col("active_loan_count")
            != (
                F.col("current_loan_count")
                + F.col("dpd30_loan_count")
                + F.col("dpd60_loan_count")
                + F.col("dpd90plus_loan_count")
                + F.col("unknown_delinquency_count")
            )
        )
        .count()
    )

    if invalid_reconciliation > 0:
        errors.append(
            "Portfolio: delinquency buckets do not "
            "reconcile to active loans."
        )


    # Prepayment is only one type of exit
    invalid_prepay = (
        portfolio
        .filter(
            F.col("prepay_count")
            >
            (
                F.col("observed_loan_count")
                - F.col("active_loan_count")
            )
        )
        .count()
    )

    if invalid_prepay > 0:
        errors.append(
            "Portfolio: prepay count exceeds total exits."
        )


    # All portfolio observations should equal source rows
    portfolio_observed = (
        portfolio
        .agg(
            F.sum(
                "observed_loan_count"
            ).alias("n")
        )
        .first()["n"]
    )

    if portfolio_observed != perf_count:
        errors.append(
            "Portfolio: summed observed loans "
            "do not equal Perf row count."
        )


    print(
        f"Portfolio observed rows: "
        f"{portfolio_observed:,}"
    )


    # ========================================================
    # 3. Delinquency Transition
    # ========================================================

    print("\n[2] Delinquency Transition")


    allowed_states = [
        "Current",
        "30 DPD",
        "60 DPD",
        "90+ DPD",
        "Exit",
        "Unknown",
    ]


    invalid_states = (
        transition
        .filter(
            (~F.col("from_state").isin(allowed_states))
            |
            (~F.col("to_state").isin(allowed_states))
        )
        .count()
    )

    if invalid_states > 0:
        errors.append(
            "Transition: unexpected delinquency state found."
        )


    # One row per period × transition
    duplicate_transitions = (
        transition
        .groupBy(
            "period",
            "from_state",
            "to_state",
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_transitions > 0:
        errors.append(
            "Transition: duplicate transition rows found."
        )


    transition_count = (
        transition
        .agg(
            F.sum(
                "transition_loan_count"
            ).alias("n")
        )
        .first()["n"]
    )


    # Maximum possible transitions:
    # each loan's first observation has no prior month
    max_possible_transitions = (
        perf_count
        - perf_loan_count
    )

    if transition_count > max_possible_transitions:
        errors.append(
            "Transition: more transitions than possible."
        )


    print(
        f"Transition observations: "
        f"{transition_count:,}"
    )

    print(
        f"Maximum possible:        "
        f"{max_possible_transitions:,}"
    )


    # ========================================================
    # 4. Loan Lifecycle
    # ========================================================

    print("\n[3] Loan Lifecycle")


    lifecycle_count = (
        lifecycle.count()
    )


    if lifecycle_count != perf_loan_count:
        errors.append(
            "Lifecycle: row count does not equal "
            "distinct Perf loan count."
        )


    # One row per loan
    duplicate_loans = (
        lifecycle
        .groupBy(
            "loan_identifier"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    if duplicate_loans > 0:
        errors.append(
            "Lifecycle: duplicate loans found."
        )


    # Observation periods
    invalid_observation_period = (
        lifecycle
        .filter(
            F.col("first_observed_period")
            > F.col("last_observed_period")
        )
        .count()
    )

    if invalid_observation_period > 0:
        errors.append(
            "Lifecycle: first observed period "
            "is after last observed period."
        )


    # DPD hierarchy
    invalid_flags = (
        lifecycle
        .filter(
            (F.col("ever_30dpd") < F.col("ever_60dpd"))
            |
            (F.col("ever_60dpd") < F.col("ever_90dpd"))
        )
        .count()
    )

    if invalid_flags > 0:
        errors.append(
            "Lifecycle: DPD flags violate "
            "30+ >= 60+ >= 90+ hierarchy."
        )


    # First delinquency periods should follow same hierarchy
    invalid_first_dpd = (
        lifecycle
        .filter(
            (
                F.col("first_60dpd_period").isNotNull()
                & (
                    F.col("first_30dpd_period").isNull()
                    |
                    (
                        F.col("first_30dpd_period")
                        > F.col("first_60dpd_period")
                    )
                )
            )
            |
            (
                F.col("first_90dpd_period").isNotNull()
                & (
                    F.col("first_60dpd_period").isNull()
                    |
                    (
                        F.col("first_60dpd_period")
                        > F.col("first_90dpd_period")
                    )
                )
            )
        )
        .count()
    )

    if invalid_first_dpd > 0:
        errors.append(
            "Lifecycle: first delinquency periods "
            "are inconsistent."
        )


    # Terminal period only when loan actually terminates
    invalid_terminal = (
        lifecycle
        .filter(
            (
                F.col("zero_balance_code").isNull()
                & F.col("terminal_period").isNotNull()
            )
            |
            (
                F.col("zero_balance_code").isNotNull()
                & (
                    F.col("terminal_period").isNull()
                    |
                    (
                        F.col("terminal_period")
                        != F.col("last_observed_period")
                    )
                )
            )
        )
        .count()
    )

    if invalid_terminal > 0:
        errors.append(
            "Lifecycle: terminal information inconsistent."
        )


    # Total lifecycle months should reconcile to Perf rows
    lifecycle_months = (
        lifecycle
        .agg(
            F.sum(
                "total_observed_months"
            ).alias("n")
        )
        .first()["n"]
    )

    if lifecycle_months != perf_count:
        errors.append(
            "Lifecycle: total observed months "
            "do not equal Perf rows."
        )


    print(
        f"Lifecycle loans:         "
        f"{lifecycle_count:,}"
    )

    print(
        f"Lifecycle months:        "
        f"{lifecycle_months:,}"
    )


    # ========================================================
    # 5. Result
    # ========================================================

    print("\n" + "=" * 70)

    if errors:

        print("VALIDATION FAILED")

        for error in errors:
            print(f"- {error}")

        raise ValueError(
            f"{len(errors)} validation check(s) failed."
        )

    else:

        print("ALL PERF MART VALIDATIONS PASSED")

    print("=" * 70)

# ============================================================
# Configuration
# ============================================================

YEAR = "2018"
QUARTER = "Q1"

PERF_PATH = (
    Path("data/processed/perf")
    / f"vintage_year={YEAR}"
    / f"vintage_quarter={QUARTER}"
)


OUTPUT_ROOT = Path(
    "data/processed/test_marts"
)

WRITE_OUTPUT = False

# ensure_folder_exists(
#         str(OUTPUT_ROOT)
#     )

# ============================================================
# Spark
# ============================================================

spark = create_spark_session(
    app_name="FreddieMacTestPerfMarts"
)


try:

    # ========================================================
    # 1. Read Perf ONCE
    # ========================================================

    perf = (
        spark
        .read
        .parquet(
            str(PERF_PATH)
        )
        .persist()
    )

    perf_count = perf.count()

    print(
        f"Perf rows: {perf_count:,}"
    )


    # ========================================================
    # 2. Portfolio Monthly
    # ========================================================

    print("\n" + "=" * 70)
    print("PORTFOLIO MONTHLY")
    print("=" * 70)

    portfolio = (
        build_portfolio_monthly(
            perf
        )
    )

    portfolio.printSchema()

    (
        portfolio
        .orderBy("period")
        .show(
            20,
            truncate=False,
        )
    )


    if WRITE_OUTPUT:

        (
            portfolio
            .write
            .mode("overwrite")
            .parquet(
                str(
                    OUTPUT_ROOT
                    / "portfolio_monthly"
                )
            )
        )


    # ========================================================
    # 3. Delinquency Transition
    # ========================================================

    print("\n" + "=" * 70)
    print("DELINQUENCY TRANSITION")
    print("=" * 70)
    
    transition = (
        build_delinquency_transition(
            perf
        )
    )
    
    transition.printSchema()
    
    (
        transition
        .orderBy(
            "period",
            "from_state",
            "to_state",
        )
        .show(
            20,
            truncate=False,
        )
    )
    
    
    if WRITE_OUTPUT:
    
        (
            transition
            .write
            .mode("overwrite")
            .parquet(
                str(
                    OUTPUT_ROOT
                    / "delinquency_transition"
                )
            )
        )


    # ========================================================
    # 4. Loan Lifecycle
    # ========================================================

    print("\n" + "=" * 70)
    print("LOAN LIFECYCLE")
    print("=" * 70)
    
    lifecycle = (
        build_loan_lifecycle(
            perf
        )
    )
    
    lifecycle.printSchema()
    
    (
        lifecycle
        .show(
            20,
            truncate=False,
        )
    )
    
    
    if WRITE_OUTPUT:
    
        (
            lifecycle
            .write
            .mode("overwrite")
            .parquet(
                str(
                    OUTPUT_ROOT
                    / "loan_lifecycle"
                )
            )
        )

    validate_perf_marts(
    perf=perf,
    portfolio=portfolio,
    transition=transition,
    lifecycle=lifecycle,
    )



finally:

    perf.unpersist()

    spark.stop()

