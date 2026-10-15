from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_portfolio_monthly(
    perf: DataFrame,
) -> DataFrame:
    """
    Build monthly portfolio monitoring metrics
    for one vintage Perf partition.

    Input grain:
        1 row per loan per reporting month

    Output grain:
        1 row per reporting month
        within the current vintage
    """

    # ========================================================
    # 1. Standardize fields used by this mart
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
            F.coalesce(
                F.col(
                    "current_actual_upb"
                ).cast("double"),
                F.lit(0.0),
            )
        )
        .withColumn(
            "is_active",
            F.when(
                F.col("zero_balance_code").isNull(),
                1,
            ).otherwise(0)
        )
    )


    # ========================================================
    # 2. Monthly aggregation
    # ========================================================

    portfolio = (
        base
        .groupBy(
            "period"
        )
        .agg(

            # ------------------------------------------------
            # Portfolio size
            # ------------------------------------------------

            F.count("*").alias(
                "observed_loan_count"
            ),

            F.sum(
                "is_active"
            ).alias(
                "active_loan_count"
            ),

            F.sum(
                F.when(
                    F.col("is_active") == 1,
                    F.col("current_upb"),
                ).otherwise(0.0)
            ).alias(
                "active_upb"
            ),


            # ------------------------------------------------
            # Current
            # ------------------------------------------------

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") == 0),
                    1,
                ).otherwise(0)
            ).alias(
                "current_loan_count"
            ),

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") == 0),
                    F.col("current_upb"),
                ).otherwise(0.0)
            ).alias(
                "current_upb"
            ),


            # ------------------------------------------------
            # 30 DPD
            # ------------------------------------------------

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") == 1),
                    1,
                ).otherwise(0)
            ).alias(
                "dpd30_loan_count"
            ),

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") == 1),
                    F.col("current_upb"),
                ).otherwise(0.0)
            ).alias(
                "dpd30_upb"
            ),


            # ------------------------------------------------
            # 60 DPD
            # ------------------------------------------------

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") == 2),
                    1,
                ).otherwise(0)
            ).alias(
                "dpd60_loan_count"
            ),

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") == 2),
                    F.col("current_upb"),
                ).otherwise(0.0)
            ).alias(
                "dpd60_upb"
            ),


            # ------------------------------------------------
            # 90+ DPD
            # ------------------------------------------------

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") >= 3),
                    1,
                ).otherwise(0)
            ).alias(
                "dpd90plus_loan_count"
            ),

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & (F.col("delinquency_level") >= 3),
                    F.col("current_upb"),
                ).otherwise(0.0)
            ).alias(
                "dpd90plus_upb"
            ),


            # ------------------------------------------------
            # Unknown / special delinquency status
            # RA / XX / NULL
            # ------------------------------------------------

            F.sum(
                F.when(
                    (F.col("is_active") == 1)
                    & F.col("delinquency_level").isNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "unknown_delinquency_count"
            ),


            # ------------------------------------------------
            # Portfolio exits
            # ------------------------------------------------

            F.sum(
                F.when(
                    F.col("zero_balance_code") == "01",
                    1,
                ).otherwise(0)
            ).alias(
                "prepay_count"
            ),
        )
        .orderBy(
            "period"
        )
    )

    return portfolio