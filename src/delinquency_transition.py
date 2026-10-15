from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_delinquency_transition(
    perf: DataFrame,
) -> DataFrame:
    """
    Build monthly delinquency transition metrics
    for one vintage Perf partition.

    Input grain:
        1 row per loan per reporting month

    Output grain:
        1 row per period × from_state × to_state
        within the current vintage
    """

    # ========================================================
    # 1. Build delinquency state
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
            "period_date",
            F.to_date(
                F.concat(
                    F.col("period").cast("string"),
                    F.lit("01"),
                ),
                "yyyyMMdd",
            )
        )
        .withColumn(
            "current_upb",
            F.coalesce(
                F.col("current_actual_upb").cast("double"),
                F.lit(0.0),
            )
        )
        .withColumn(
            "state",
            F.when(
                F.col("zero_balance_code").isNotNull(),
                "Exit",
            )
            .when(
                F.col("current_loan_delinquency_status") == "00",
                "Current",
            )
            .when(
                F.col("current_loan_delinquency_status") == "01",
                "30 DPD",
            )
            .when(
                F.col("current_loan_delinquency_status") == "02",
                "60 DPD",
            )
            .when(
                F.col(
                    "current_loan_delinquency_status"
                ).rlike(r"^[0-9]+$")
                & (
                    F.col(
                        "current_loan_delinquency_status"
                    ).cast("int") >= 3
                ),
                "90+ DPD",
            )
            .otherwise(
                "Unknown"
            )
        )
    )


    # ========================================================
    # 2. Get previous-month state
    # ========================================================

    window = (
        Window
        .partitionBy(
            "loan_identifier"
        )
        .orderBy(
            "period"
        )
    )

    transitions = (
        base
        .withColumn(
            "previous_state",
            F.lag(
                "state"
            ).over(window)
        )
        .withColumn(
            "previous_period_date",
            F.lag(
                "period_date"
            ).over(window)
        )
        .withColumn(
            "previous_upb",
            F.lag(
                "current_upb"
            ).over(window)
        )
    )


    # ========================================================
    # 3. Keep valid consecutive-month transitions
    # ========================================================

    transitions = (
        transitions
        .filter(
            F.col("previous_state").isNotNull()
        )
        .filter(
            F.col("period_date")
            ==
            F.add_months(
                F.col("previous_period_date"),
                1,
            )
        )
    )


    # ========================================================
    # 4. Aggregate transitions
    # ========================================================

    result = (
        transitions
        .groupBy(
            "period",
            F.col("previous_state").alias(
                "from_state"
            ),
            F.col("state").alias(
                "to_state"
            ),
        )
        .agg(
            F.count("*").alias(
                "transition_loan_count"
            ),

            F.sum(
                "previous_upb"
            ).alias(
                "from_upb"
            ),

            F.sum(
                "current_upb"
            ).alias(
                "to_upb"
            ),
        )
        .orderBy(
            "period",
            "from_state",
            "to_state",
        )
    )

    return result