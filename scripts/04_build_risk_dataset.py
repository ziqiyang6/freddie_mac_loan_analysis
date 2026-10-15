from pathlib import Path

from src.utils import ensure_folder_exists, create_spark_session


# ============================================================
# Configuration
# ============================================================

ORIG_ROOT = Path(
    "data/processed/orig"
)

TARGET_ROOT = Path(
    "data/processed/loan_targets"
)

OUTPUT_ROOT = Path(
    "data/processed/risk_analysis"
)

FORCE_REPROCESS = False


# ============================================================
# 1. Discover Orig partitions
# ============================================================

def discover_orig_partitions():

    partitions = []

    for year_dir in sorted(
        ORIG_ROOT.glob("vintage_year=*")
    ):

        if not year_dir.is_dir():
            continue

        year = year_dir.name.split(
            "=",
            1
        )[1]


        for quarter_dir in sorted(
            year_dir.glob("vintage_quarter=*")
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
# 2. Join one vintage
# ============================================================

def build_risk_partition(
    spark,
    year,
    quarter,
    orig_path,
    force=False,
):

    target_path = (
        TARGET_ROOT
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )

    output_path = (
        OUTPUT_ROOT
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )

    success_file = (
        output_path / "_SUCCESS"
    )


    print("\n" + "=" * 70)
    print(f"Processing: {year}{quarter}")
    print(f"Orig:       {orig_path}")
    print(f"Targets:    {target_path}")
    print(f"Output:     {output_path}")
    print("=" * 70)


    # ========================================================
    # Skip completed partition
    # ========================================================

    if (
        success_file.exists()
        and not force
    ):

        print(
            f"SKIP: {year}{quarter} "
            "already exists."
        )

        return "skipped"


    # ========================================================
    # Check corresponding target partition
    # ========================================================

    if not target_path.exists():

        raise FileNotFoundError(
            f"Target partition not found: "
            f"{target_path}"
        )


    # ========================================================
    # Read Orig + Targets
    # ========================================================

    orig = spark.read.parquet(
        str(orig_path)
    )

    targets = spark.read.parquet(
        str(target_path)
    )


    orig.createOrReplaceTempView(
        "orig"
    )

    targets.createOrReplaceTempView(
        "targets"
    )


    # ========================================================
    # Row counts before JOIN
    # ========================================================

    orig_count = orig.count()
    target_count = targets.count()

    print(
        f"Orig rows:   {orig_count:,}"
    )

    print(
        f"Target rows: {target_count:,}"
    )


    # ========================================================
    # JOIN
    # ========================================================

    risk = spark.sql("""
        SELECT

            -- ==============================================
            -- Key
            -- ==============================================

            o.loan_identifier,


            -- ==============================================
            -- Origination credit characteristics
            -- ==============================================

            o.classic_fico,
            o.vantagescore_4_0,

            o.original_dti,
            o.original_ltv,
            o.original_cltv,

            o.original_upb,
            o.original_interest_rate,

            o.mortgage_insurance_percentage,

            o.number_of_borrowers,
            o.number_of_units,

            o.first_time_homebuyer_indicator,

            o.occupancy_status,
            o.loan_purpose,
            o.channel,

            o.property_type,
            o.property_state,

            o.original_loan_term,


            -- ==============================================
            -- Observation information
            -- ==============================================

            t.total_observed_months,

            t.observed_months_12m,
            t.observed_months_24m,

            t.first_observed_period,


            -- ==============================================
            -- Maximum delinquency
            -- ==============================================

            t.max_delinquency_12m,
            t.max_delinquency_24m,


            -- ==============================================
            -- 12M outcomes
            -- ==============================================

            t.ever_30dpd_12m,
            t.ever_60dpd_12m,
            t.ever_90dpd_12m,


            -- ==============================================
            -- 24M outcomes
            -- ==============================================

            t.ever_30dpd_24m,
            t.ever_60dpd_24m,
            t.ever_90dpd_24m


        FROM orig o

        INNER JOIN targets t

            ON o.loan_identifier
             = t.loan_identifier
    """)


    # ========================================================
    # Validate JOIN
    # ========================================================

    joined_count = risk.count()

    print(
        f"Joined rows: {joined_count:,}"
    )


    if joined_count != orig_count:

        raise ValueError(
            f"{year}{quarter}: "
            f"JOIN validation failed. "
            f"Orig={orig_count:,}, "
            f"Target={target_count:,}, "
            f"Joined={joined_count:,}"
        )


    # ========================================================
    # Write output
    # ========================================================

    (
        risk
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
            "_SUCCESS not found."
        )


    print(
        f"SUCCESS: {year}{quarter}"
    )

    return "processed"


# ============================================================
# 3. Main
# ============================================================

def main():

    ensure_folder_exists(
        str(OUTPUT_ROOT)
    )


    partitions = (
        discover_orig_partitions()
    )


    if not partitions:

        raise FileNotFoundError(
            f"No Orig partitions found "
            f"under {ORIG_ROOT}"
        )


    print(
        f"Found {len(partitions)} "
        "Orig partitions."
    )


    spark = create_spark_session(
        app_name="FreddieMacBuildRiskDataset"
    )


    processed = 0
    skipped = 0


    try:

        for index, (
            year,
            quarter,
            orig_path,
        ) in enumerate(
            partitions,
            start=1
        ):

            print(
                f"\n[{index}/{len(partitions)}]"
            )


            status = build_risk_partition(
                spark=spark,
                year=year,
                quarter=quarter,
                orig_path=orig_path,
                force=FORCE_REPROCESS,
            )


            if status == "processed":
                processed += 1

            elif status == "skipped":
                skipped += 1


    finally:

        spark.stop()


    print("\n" + "=" * 70)
    print("RISK DATASET BUILD FINISHED")
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