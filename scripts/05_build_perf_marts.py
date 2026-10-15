from pathlib import Path

from pyspark import StorageLevel

from src.utils import (
    ensure_folder_exists,
    create_spark_session,
)

from src.portfolio_monthly import (
    build_portfolio_monthly,
)

from src.delinquency_transition import (
    build_delinquency_transition,
)

from src.loan_lifecycle import (
    build_loan_lifecycle,
)


# ============================================================
# Configuration
# ============================================================

PERF_ROOT = Path(
    "data/processed/perf"
)

OUTPUT_ROOTS = {
    "portfolio_monthly": Path(
        "data/processed/portfolio_monthly"
    ),

    "delinquency_transition": Path(
        "data/processed/delinquency_transition"
    ),

    "loan_lifecycle": Path(
        "data/processed/loan_lifecycle"
    ),
}

FORCE_REPROCESS = False


# ============================================================
# 1. Discover Perf partitions
# ============================================================

def discover_perf_partitions():

    partitions = []

    for year_dir in sorted(
        PERF_ROOT.glob("vintage_year=*")
    ):

        if not year_dir.is_dir():
            continue

        year = year_dir.name.split(
            "=",
            1,
        )[1]

        for quarter_dir in sorted(
            year_dir.glob("vintage_quarter=*")
        ):

            if not quarter_dir.is_dir():
                continue

            quarter = quarter_dir.name.split(
                "=",
                1,
            )[1]

            partitions.append(
                (
                    year,
                    quarter,
                    quarter_dir,
                )
            )

    return partitions


# ============================================================
# 2. Process one Perf vintage
# ============================================================

def process_perf_partition(
    spark,
    year,
    quarter,
    perf_path,
    force=False,
):

    portfolio_path = (
        OUTPUT_ROOTS["portfolio_monthly"]
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )

    transition_path = (
        OUTPUT_ROOTS["delinquency_transition"]
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )

    lifecycle_path = (
        OUTPUT_ROOTS["loan_lifecycle"]
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )


    # ========================================================
    # Skip completed vintage
    # ========================================================

    success_files = [
        portfolio_path / "_SUCCESS",
        transition_path / "_SUCCESS",
        lifecycle_path / "_SUCCESS",
    ]

    if (
        all(
            path.exists()
            for path in success_files
        )
        and not force
    ):

        print(
            f"SKIP: {year}{quarter} "
            "already completed."
        )

        return "skipped"


    print("\n" + "=" * 70)
    print(f"Processing: {year}{quarter}")
    print(f"Perf:       {perf_path}")
    print("=" * 70)


    # ========================================================
    # Read Perf ONCE
    # ========================================================

    perf = (
        spark
        .read
        .parquet(
            str(perf_path)
        )
        .select(
            "loan_identifier",
            "period",
            "current_actual_upb",
            "current_loan_delinquency_status",
            "zero_balance_code",
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )


    # Materialize cache
    perf_count = perf.count()

    print(
        f"Perf rows: {perf_count:,}"
    )


    try:

        # ====================================================
        # Mart 1: Portfolio Monthly
        # ====================================================

        portfolio = (
            build_portfolio_monthly(
                perf
            )
        )


        # ====================================================
        # Mart 2: Delinquency Transition
        # ====================================================

        transition = (
            build_delinquency_transition(
                perf
            )
        )


        # ====================================================
        # Mart 3: Loan Lifecycle
        # ====================================================

        lifecycle = (
            build_loan_lifecycle(
                perf
            )
        )


        # ====================================================
        # Write Portfolio Monthly
        # ====================================================

        (
            portfolio
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(
                str(portfolio_path)
            )
        )

        print(
            "Portfolio monthly written."
        )


        # ====================================================
        # Write Delinquency Transition
        # ====================================================

        (
            transition
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(
                str(transition_path)
            )
        )

        print(
            "Delinquency transition written."
        )


        # ====================================================
        # Write Loan Lifecycle
        # ====================================================

        (
            lifecycle
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy",
            )
            .parquet(
                str(lifecycle_path)
            )
        )

        print(
            "Loan lifecycle written."
        )


    finally:

        perf.unpersist()


    print(
        f"SUCCESS: {year}{quarter}"
    )

    return "processed"


# ============================================================
# 3. Main
# ============================================================

def main():

    # ========================================================
    # Create output roots
    # ========================================================

    for output_root in (
        OUTPUT_ROOTS.values()
    ):

        ensure_folder_exists(
            str(output_root)
        )


    # ========================================================
    # Discover partitions
    # ========================================================

    partitions = (
        discover_perf_partitions()
    )

    if not partitions:

        raise FileNotFoundError(
            f"No Perf partitions found "
            f"under {PERF_ROOT}"
        )


    print(
        f"Found {len(partitions)} "
        "Perf partitions."
    )


    # ========================================================
    # Spark
    # ========================================================

    spark = create_spark_session(
        app_name="FreddieMacBuildPerfMarts"
    )


    processed = 0
    skipped = 0


    try:

        for index, (
            year,
            quarter,
            perf_path,
        ) in enumerate(
            partitions,
            start=1,
        ):

            print(
                f"\n[{index}/{len(partitions)}]"
            )


            status = (
                process_perf_partition(
                    spark=spark,
                    year=year,
                    quarter=quarter,
                    perf_path=perf_path,
                    force=FORCE_REPROCESS,
                )
            )


            if status == "processed":
                processed += 1

            elif status == "skipped":
                skipped += 1


    finally:

        spark.stop()


    # ========================================================
    # Summary
    # ========================================================

    print("\n" + "=" * 70)
    print("PERF MART BUILD FINISHED")
    print("=" * 70)

    print(
        f"Total:     {len(partitions)}"
    )

    print(
        f"Processed: {processed}"
    )

    print(
        f"Skipped:   {skipped}"
    )


if __name__ == "__main__":
    main()