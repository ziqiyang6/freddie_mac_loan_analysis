from pathlib import Path

from src.utils import (create_spark_session
    ,load_schema,
    schema_validation, process_file, ensure_folder_exists
)


CONFIG_PATH = (
    "config/schemas/"
    "freddie_mac_release47.json"
)

INPUT_ROOT = Path(
    "data/raw_txt"
)

OUTPUT_ROOT = (
    "data/processed/orig"
)


def main():
    # ========================================================
    # 0. DIR Checking
    # ========================================================
    ensure_folder_exists(OUTPUT_ROOT)


    # ========================================================
    # 1. Load schema config
    # ========================================================

    config = load_schema(
        CONFIG_PATH
    )

    schema_validation(
        config
    )

    orig_config = config["orig"]


    # ========================================================
    # 2. Find all origination files
    # ========================================================

    orig_files = sorted(
        INPUT_ROOT.glob(
            "*/*/orig_*.txt"
        )
    )

    if not orig_files:
        raise FileNotFoundError(
            "No origination files found."
        )


    print(
        f"Found {len(orig_files)} "
        "Origination files."
    )


    # ========================================================
    # 3. Create ONE Spark session
    # ========================================================

    spark = create_spark_session(
        app_name="FreddieMacOrigProcessing"
    )


    processed = 0
    skipped = 0


    try:

        # ====================================================
        # 4. Process files one by one
        # ====================================================

        for index, orig_file in enumerate(
            orig_files,
            start=1
        ):

            print(
                f"\n[{index}/{len(orig_files)}]"
            )

            status = process_file(
                spark=spark,
                input_file=orig_file,
                dataset_config=orig_config,
                output_root=OUTPUT_ROOT,
                force=False,
            )


            if status == "processed":
                processed += 1

            elif status == "skipped":
                skipped += 1


    finally:

        spark.stop()


    # ========================================================
    # 5. Summary
    # ========================================================

    print("\n" + "=" * 60)
    print("ORIGINATION ETL FINISHED")
    print("=" * 60)

    print(
        f"Total files: {len(orig_files)}"
    )

    print(
        f"Processed:   {processed}"
    )

    print(
        f"Skipped:     {skipped}"
    )


if __name__ == "__main__":
    main()