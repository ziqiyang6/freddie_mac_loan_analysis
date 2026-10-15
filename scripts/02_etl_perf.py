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
    "data/processed/perf"
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

    perf_config = config["perf"]


    # ========================================================
    # 2. Find all performance files
    # ========================================================

    perf_files = sorted(
        INPUT_ROOT.glob(
            "*/*/perf_*.txt"
        )
    )

    if not perf_files:
        raise FileNotFoundError(
            "No performance files found."
        )


    print(
        f"Found {len(perf_files)} "
        "performance files."
    )


    # ========================================================
    # 3. Create ONE Spark session
    # ========================================================

    spark = create_spark_session(
        app_name="FreddieMacPerfProcessing"
    )


    processed = 0
    skipped = 0


    try:

        # ====================================================
        # 4. Process files one by one
        # ====================================================

        for index, perf_file in enumerate(
            perf_files,
            start=1
        ):

            print(
                f"\n[{index}/{len(perf_files)}]"
            )

            status = process_file(
                spark=spark,
                input_file=perf_file,
                dataset_config=perf_config,
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
    print("PERFORMANCE ETL FINISHED")
    print("=" * 60)

    print(
        f"Total files: {len(perf_files)}"
    )

    print(
        f"Processed:   {processed}"
    )

    print(
        f"Skipped:     {skipped}"
    )


if __name__ == "__main__":
    main()