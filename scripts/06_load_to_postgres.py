from getpass import getpass

from src.utils import create_spark_session


# ============================================================
# Configuration
# ============================================================

JDBC_URL = (
    "jdbc:postgresql://localhost:5432/freddie_mac_risk"
    "?reWriteBatchedInserts=true"
)

DB_USER = "freddie_user"


MARTS = {
    "risk_analysis": {
        "path": "data/processed/risk_analysis",
        "table": "risk.risk_analysis",
        "partitions": 4,
    },

    "portfolio_monthly": {
        "path": "data/processed/portfolio_monthly",
        "table": "risk.portfolio_monthly",
        "partitions": 2,
    },

    "delinquency_transition": {
        "path": "data/processed/delinquency_transition",
        "table": "risk.delinquency_transition",
        "partitions": 2,
    },

    "loan_lifecycle": {
        "path": "data/processed/loan_lifecycle",
        "table": "risk.loan_lifecycle",
        "partitions": 4,
    },
}


# ============================================================
# JDBC Writer
# ============================================================

def load_to_postgres(
    df,
    table_name,
    db_password,
    num_partitions,
):

    print("\n" + "=" * 70)
    print(f"Loading {table_name}")
    print("=" * 70)

    row_count = df.count()

    print(
        f"Rows: {row_count:,}"
    )

    (
        df
        .coalesce(num_partitions)
        .write
        .format("jdbc")
        .option(
            "url",
            JDBC_URL
        )
        .option(
            "dbtable",
            table_name
        )
        .option(
            "user",
            DB_USER
        )
        .option(
            "password",
            db_password
        )
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .option(
            "batchsize",
            "10000"
        )
        .mode("append")
        .save()
    )

    print(
        f"Finished: {table_name}"
    )


# ============================================================
# Main
# ============================================================

def main():

    db_password = getpass(
        "PostgreSQL password: "
    )

    spark = create_spark_session(
        app_name="FreddieMacLoadRiskMarts",
        packages="org.postgresql:postgresql:42.7.4",
    )

    try:

        for mart_name, config in MARTS.items():
            
            

            print(
                f"\nReading {mart_name}"
            )

            df = (
                spark
                .read
                .parquet(
                    config["path"]
                )
            )

            load_to_postgres(
                df=df,
                table_name=config["table"],
                db_password=db_password,
                num_partitions=config["partitions"],
            )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()