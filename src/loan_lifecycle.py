from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_loan_lifecycle(
    perf: DataFrame,
) -> DataFrame:
    """
    Build loan-level lifecycle metrics
    from one vintage Perf partition.

    Input grain:
        1 row per loan per reporting month

    Output grain:
        1 row per loan
    """

    # ========================================================
    # 1. Standardize fields
    # ========================================================

    base = (
        perf
        .select(
            "loan_identifier",
            "period",
            "current_actual_upb",
            "current_loan_delinquency_status",
            "zero_balance_code",
        )
        .withColumn(
            "delinquency_level",
            F.when(
                F.col(
                    "current_loan_delinquency_status"
                ).rlike(r"^[0-9]+$"),
                F.col(
                    "current_loan_delinquency_status"
                ).cast("int"),
            )
        )
        .withColumn(
            "current_upb",
            F.col(
                "current_actual_upb"
            ).cast("double")
        )
    )


    # ========================================================
    # 2. Aggregate lifecycle performance
    # ========================================================

    lifecycle = (
        base
        .groupBy(
            "loan_identifier"
        )
        .agg(

            # ------------------------------------------------
            # Observation window
            # ------------------------------------------------

            F.min(
                "period"
            ).alias(
                "first_observed_period"
            ),

            F.max(
                "period"
            ).alias(
                "last_observed_period"
            ),

            F.count(
                "*"
            ).alias(
                "total_observed_months"
            ),


            # ------------------------------------------------
            # First delinquency events
            # ------------------------------------------------

            F.min(
                F.when(
                    F.col("delinquency_level") >= 1,
                    F.col("period"),
                )
            ).alias(
                "first_30dpd_period"
            ),

            F.min(
                F.when(
                    F.col("delinquency_level") >= 2,
                    F.col("period"),
                )
            ).alias(
                "first_60dpd_period"
            ),

            F.min(
                F.when(
                    F.col("delinquency_level") >= 3,
                    F.col("period"),
                )
            ).alias(
                "first_90dpd_period"
            ),


            # ------------------------------------------------
            # Worst delinquency
            # ------------------------------------------------

            F.max(
                "delinquency_level"
            ).alias(
                "max_delinquency"
            ),


            # ------------------------------------------------
            # Lifetime delinquency flags
            # ------------------------------------------------

            F.max(
                F.when(
                    F.col("delinquency_level") >= 1,
                    1,
                ).otherwise(0)
            ).alias(
                "ever_30dpd"
            ),

            F.max(
                F.when(
                    F.col("delinquency_level") >= 2,
                    1,
                ).otherwise(0)
            ).alias(
                "ever_60dpd"
            ),

            F.max(
                F.when(
                    F.col("delinquency_level") >= 3,
                    1,
                ).otherwise(0)
            ).alias(
                "ever_90dpd"
            ),
        )
    )


    # ========================================================
    # 3. Get final observed record
    # ========================================================

    window = (
        Window
        .partitionBy(
            "loan_identifier"
        )
        .orderBy(
            F.col("period").desc()
        )
    )

    last_record = (
        base
        .withColumn(
            "_row_number",
            F.row_number().over(
                window
            )
        )
        .filter(
            F.col("_row_number") == 1
        )
        .select(
            "loan_identifier",

            F.when(
                F.col(
                    "zero_balance_code"
                ).isNotNull(),
                F.col("period"),
            ).alias(
                "terminal_period"
            ),

            F.col(
                "zero_balance_code"
            ).alias(
                "zero_balance_code"
            ),

            F.col(
                "current_upb"
            ).alias(
                "last_observed_upb"
            ),
        )
    )


    # ========================================================
    # 4. Combine lifecycle + terminal information
    # ========================================================

    result = (
        lifecycle
        .join(
            last_record,
            on="loan_identifier",
            how="left",
        )
    )

    return result