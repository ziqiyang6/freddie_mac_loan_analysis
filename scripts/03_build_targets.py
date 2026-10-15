from pathlib import Path

from src.utils import create_spark_session, ensure_folder_exists


# ============================================================
# Configuration
# ============================================================

PERF_ROOT = Path(
    "data/processed/perf"
)

TARGET_ROOT = Path(
    "data/processed/loan_targets"
)

ensure_folder_exists(TARGET_ROOT)

FORCE_REPROCESS = False


# ============================================================
# 1. Find all Perf partitions
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
            1
        )[1]


        for quarter_dir in sorted(
            year_dir.glob(
                "vintage_quarter=*"
            )
        ):

            if not quarter_dir.is_dir():
                continue

            quarter = quarter_dir.name.split(
                "=",
                1
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
# 2. Process one Perf partition
# ============================================================

def build_targets_for_partition(
    spark,
    year,
    quarter,
    perf_path,
    force=False,
):

    output_path = (
        TARGET_ROOT
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )

    success_file = (
        output_path / "_SUCCESS"
    )


    print("\n" + "=" * 70)

    print(
        f"Processing: {year}{quarter}"
    )

    print(
        f"Input:      {perf_path}"
    )

    print(
        f"Output:     {output_path}"
    )

    print("=" * 70)


    # ========================================================
    # Skip completed target partition
    # ========================================================

    if (
        success_file.exists()
        and not force
    ):

        print(
            f"SKIP: targets for "
            f"{year}{quarter} already exist."
        )

        return "skipped"


    # ========================================================
    # Read this Perf quarter
    # ========================================================

    perf = spark.read.parquet(
        str(perf_path)
    )


    # SQL treats this DataFrame as a temporary table
    perf.createOrReplaceTempView(
        "perf"
    )


    # ========================================================
    # Build loan-level outcomes
    # ========================================================

    targets = spark.sql("""
        WITH perf_clean AS (

            SELECT
                loan_identifier,
                period,
                current_loan_delinquency_status,
                modification_flag,
                zero_balance_code,

                CASE
                    WHEN current_loan_delinquency_status
                         RLIKE '^[0-9]+$'

                    THEN CAST(
                        current_loan_delinquency_status
                        AS INT
                    )

                    ELSE NULL

                END AS delinquency_level,


                ROW_NUMBER() OVER (

                    PARTITION BY
                        loan_identifier

                    ORDER BY
                        period

                ) AS observed_month_num

            FROM perf
        )


        SELECT
            loan_identifier,


            -- ==============================================
            -- Observation information
            -- ==============================================

            COUNT(*) AS total_observed_months,


            SUM(
                CASE
                    WHEN observed_month_num <= 12
                    THEN 1
                    ELSE 0
                END
            ) AS observed_months_12m,


            SUM(
                CASE
                    WHEN observed_month_num <= 24
                    THEN 1
                    ELSE 0
                END
            ) AS observed_months_24m,


            MIN(period)
                AS first_observed_period,


            -- ==============================================
            -- Maximum delinquency
            -- ==============================================

            MAX(
                CASE
                    WHEN observed_month_num <= 12
                    THEN delinquency_level
                END
            ) AS max_delinquency_12m,


            MAX(
                CASE
                    WHEN observed_month_num <= 24
                    THEN delinquency_level
                END
            ) AS max_delinquency_24m,


            -- ==============================================
            -- 12M outcomes
            -- ==============================================

            MAX(
                CASE
                    WHEN observed_month_num <= 12
                         AND delinquency_level >= 1
                    THEN 1
                    ELSE 0
                END
            ) AS ever_30dpd_12m,


            MAX(
                CASE
                    WHEN observed_month_num <= 12
                         AND delinquency_level >= 2
                    THEN 1
                    ELSE 0
                END
            ) AS ever_60dpd_12m,


            MAX(
                CASE
                    WHEN observed_month_num <= 12
                         AND delinquency_level >= 3
                    THEN 1
                    ELSE 0
                END
            ) AS ever_90dpd_12m,


            -- ==============================================
            -- 24M outcomes
            -- ==============================================

            MAX(
                CASE
                    WHEN observed_month_num <= 24
                         AND delinquency_level >= 1
                    THEN 1
                    ELSE 0
                END
            ) AS ever_30dpd_24m,


            MAX(
                CASE
                    WHEN observed_month_num <= 24
                         AND delinquency_level >= 2
                    THEN 1
                    ELSE 0
                END
            ) AS ever_60dpd_24m,


            MAX(
                CASE
                    WHEN observed_month_num <= 24
                         AND delinquency_level >= 3
                    THEN 1
                    ELSE 0
                END
            ) AS ever_90dpd_24m


        FROM perf_clean

        GROUP BY
            loan_identifier
    """)


    # ========================================================
    # Write target partition
    # ========================================================

    (
        targets
        .write
        .mode("overwrite")
        .option(
            "compression",
            "snappy"
        )
        .parquet(
            str(output_path)
        )
    )


    if not success_file.exists():

        raise RuntimeError(
            f"{year}{quarter}: "
            "target write completed but "
            "_SUCCESS was not found."
        )


    print(
        f"SUCCESS: {year}{quarter}"
    )

    return "processed"


# ============================================================
# 3. Main batch
# ============================================================

def main():

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


    spark = create_spark_session(
        app_name=(
            "FreddieMacBuildLoanTargets"
        )
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
            start=1
        ):

            print(
                f"\n[{index}/{len(partitions)}]"
            )


            status = (
                build_targets_for_partition(
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


    print("\n" + "=" * 70)

    print(
        "LOAN TARGET CONSTRUCTION FINISHED"
    )

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