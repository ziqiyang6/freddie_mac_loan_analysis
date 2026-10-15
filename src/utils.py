import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

def ensure_folder_exists(
    folder_path: str
) -> None:

    path = Path(folder_path)

    path.mkdir(
        parents=True,
        exist_ok=True
    )

def create_spark_session(
    app_name="FreddieMacMortgageRisk",
    master="local[4]",
    driver_memory="4g",
    shuffle_partitions="8",
    packages=None,
):

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config(
            "spark.driver.memory",
            driver_memory
        )
        .config(
            "spark.sql.shuffle.partitions",
            shuffle_partitions
        )
    )

    if packages:
        builder = builder.config(
            "spark.jars.packages",
            packages,
        )

    return builder.getOrCreate()



# ============================================================
# Supported processed datatypes
# ============================================================

SUPPORTED_PROCESSED_TYPES = {
    "string",
    "int",
    "double",
}


# ============================================================
# 1. Load JSON config
# ============================================================

def load_schema(path):
    """
    Load the complete dataset schema JSON.

    Example:
        config = load_schema_config(
            "config/schemas/freddie_mac_release47_schema.json"
        )

    Returns:
        dict
    """

    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(
            f"Schema config not found: {path}"
        )

    if path.suffix.lower() != ".json":
        raise ValueError(
            f"Schema config must be a JSON file: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8"
    ) as f:

        config = json.load(f)

    return config

def schema_validation(data):
    """
    Validate the overall JSON schema configuration.

    Checks:
    - required top-level keys
    - orig / perf existence
    - dataset-specific schema consistency

    Raises:
        ValueError if the config is invalid.
    """
    

    required_top_level_keys = {
        "dataset",
        "release",
        "effective_date",
        "raw_read_type",
        "orig",
        "perf",
    }

    missing_keys = (
        required_top_level_keys
        - set(data.keys())
    )

    if missing_keys:
        raise ValueError(
            "Missing top-level schema keys: "
            f"{sorted(missing_keys)}"
        )


    if data["raw_read_type"] != "string":
        raise ValueError(
            "Current pipeline expects "
            "raw_read_type='string'."
        )


    validate_dataset_schema(
        data["orig"],
        dataset_type="orig"
    )

    validate_dataset_schema(
        data["perf"],
        dataset_type="perf"
    )

    return True

def validate_dataset_schema(
    dataset_config,
    dataset_type
):

    required_keys = {
        "delimiter",
        "header",
        "expected_column_count",
        "file_pattern",
        "columns",
    }


    missing_keys = (
        required_keys
        - set(dataset_config.keys())
    )


    if missing_keys:

        raise ValueError(
            f"{dataset_type}: "
            f"missing config keys: "
            f"{sorted(missing_keys)}"
        )


    columns = dataset_config["columns"]

    expected_count = (
        dataset_config[
            "expected_column_count"
        ]
    )

    actual_count = len(columns)


    if actual_count != expected_count:

        raise ValueError(
            f"{dataset_type}: "
            f"expected {expected_count} columns, "
            f"but JSON defines {actual_count}."
        )


    positions = [
        field["position"]
        for field in columns
    ]

    expected_positions = list(
        range(
            1,
            expected_count + 1
        )
    )


    if sorted(positions) != expected_positions:

        raise ValueError(
            f"{dataset_type}: "
            "invalid column positions."
        )


    column_names = [
        field["name"]
        for field in columns
    ]


    if len(column_names) != len(
        set(column_names)
    ):

        duplicates = sorted({
            name
            for name in column_names
            if column_names.count(name) > 1
        })

        raise ValueError(
            f"{dataset_type}: "
            f"duplicate columns: "
            f"{duplicates}"
        )


    for field in columns:

        required_field_keys = {
            "position",
            "name",
            "processed_type",
        }


        missing_field_keys = (
            required_field_keys
            - set(field.keys())
        )


        if missing_field_keys:

            raise ValueError(
                f"{dataset_type}: "
                f"invalid column definition: "
                f"{field}. "
                f"Missing: "
                f"{sorted(missing_field_keys)}"
            )


        processed_type = (
            field["processed_type"]
        )


        if (
            processed_type
            not in SUPPORTED_PROCESSED_TYPES
        ):

            raise ValueError(
                f"{dataset_type}: "
                f"unsupported processed_type "
                f"'{processed_type}' "
                f"for '{field['name']}'."
            )


    return True

# ============================================================
# 4. JSON config → Raw Spark Schema
# ============================================================

def build_raw_spark_schema(
    dataset
):

    columns = sorted(
        dataset["columns"],
        key=lambda x: x["position"]
    )

    return StructType([
        StructField(
            field["name"],
            StringType(),
            True
        )
        for field in columns
    ])

# ============================================================
# 5. Apply processed datatypes
# ============================================================

def apply_processed_types(
    df,
    dataset_config
):

    columns = sorted(
        dataset_config["columns"],
        key=lambda x: x["position"]
    )

    for field in columns:

        name = field["name"]
        target_type = field["processed_type"]

        if target_type == "string":
            continue

        df = df.withColumn(
            name,
            col(name).cast(target_type)
        )

    return df




def get_vintage_from_path(input_file):

    input_path = Path(input_file)

    quarter = input_path.parent.name
    year = input_path.parent.parent.name

    return year, quarter

def build_output_path(
    output_root,
    year,
    quarter
):

    return (
        Path(output_root)
        / f"vintage_year={year}"
        / f"vintage_quarter={quarter}"
    )


# Processing the orig/perf file from txt to parquet, 
def process_file(
    spark,
    input_file,
    dataset_config,
    output_root,
    force=False
):

    # ========================================================
    # 1. Input / vintage information
    # ========================================================

    input_path = Path(input_file)

    year, quarter = get_vintage_from_path(
        input_path
    )


    # ========================================================
    # 2. Output path
    # ========================================================

    output_path = build_output_path(
        output_root,
        year,
        quarter
    )

    success_file = output_path / "_SUCCESS"


    print("\n" + "=" * 70)
    print(f"Input:   {input_path}")
    print(f"Vintage: {year}{quarter}")
    print(f"Output:  {output_path}")
    print("=" * 70)


    # ========================================================
    # 3. Skip completed output
    # ========================================================

    if success_file.exists() and not force:

        print(
            f"SKIP: {year}{quarter} "
            "already processed."
        )

        return "skipped"


    # ========================================================
    # 4. Detect incomplete previous run
    # ========================================================

    if (
        output_path.exists()
        and not success_file.exists()
    ):

        print(
            f"WARNING: incomplete output "
            f"found for {year}{quarter}. "
            "Reprocessing."
        )


    # ========================================================
    # 5. JSON config → Spark raw schema
    # ========================================================

    raw_schema = build_raw_spark_schema(
        dataset_config
    )


    # ========================================================
    # 6. Read raw TXT → DataFrame
    # ========================================================

    df = (
        spark.read
        .option(
            "sep",
            dataset_config["delimiter"]
        )
        .option(
            "header",
            str(
                dataset_config["header"]
            ).lower()
        )
        .option(
            "mode",
            "FAILFAST"
        )
        .schema(
            raw_schema
        )
        .csv(
            str(input_path)
        )
    )


    # ========================================================
    # 7. Apply processed datatypes
    # ========================================================

    df = apply_processed_types(
        df,
        dataset_config
    )


    # ========================================================
    # 8. Write Parquet
    # ========================================================

    (
        df
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


    # ========================================================
    # 9. Confirm success
    # ========================================================

    if not success_file.exists():

        raise RuntimeError(
            f"{year}{quarter}: "
            "write finished but "
            "_SUCCESS was not found."
        )


    print(
        f"SUCCESS: {year}{quarter}"
    )

    return "processed"