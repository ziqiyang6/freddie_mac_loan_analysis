#!/bin/bash

set -euo pipefail


# ============================================================
# Freddie Mac SFLLD Nested ZIP Extractor
#
# Input:
#
# raw_zip/
#   historical_data_2018.zip
#   historical_data_2019.zip
#   ...
#
# Each yearly ZIP contains:
#
#   historical_data_2018Q1.zip
#   historical_data_2018Q2.zip
#   historical_data_2018Q3.zip
#   historical_data_2018Q4.zip
#
# Each quarterly ZIP contains:
#
#   orig TXT
#   perf TXT
#
# Output:
#
# raw_txt/
#   2018/
#     Q1/
#       orig_2018Q1.txt
#       perf_2018Q1.txt
# ============================================================


# ============================================================
# 1. 修改路径
# ============================================================

ZIP_DIR="./data/raw_zip"
OUTPUT_DIR="./data/raw_txt"


mkdir -p "$OUTPUT_DIR"


# ============================================================
# 2. 临时目录
#
# 年度 ZIP 和季度 ZIP 都只在这里临时解压
# 脚本结束后自动删除
# ============================================================

TMP_ROOT=$(mktemp -d)

cleanup() {
    echo
    echo "Cleaning temporary files..."
    rm -rf "$TMP_ROOT"
}

trap cleanup EXIT


echo "============================================================"
echo "Freddie Mac SFLLD Nested ZIP Extractor"
echo "============================================================"
echo
echo "Year ZIP directory: $ZIP_DIR"
echo "TXT output:         $OUTPUT_DIR"
echo


# ============================================================
# 3. 统计年度 ZIP
# ============================================================

YEAR_ZIP_COUNT=$(find "$ZIP_DIR" \
    -type f \
    -name "historical_data_20??.zip" \
    | wc -l \
    | tr -d ' ')

echo "Found $YEAR_ZIP_COUNT yearly ZIP files."
echo


if [ "$YEAR_ZIP_COUNT" -eq 0 ]; then
    echo "ERROR: No yearly ZIP files found."
    exit 1
fi


YEAR_COUNT=0
QUARTER_COUNT=0


# ============================================================
# 4. 第一层：遍历年度 ZIP
# ============================================================

while IFS= read -r YEAR_ZIP
do

    YEAR_ZIP_NAME=$(basename "$YEAR_ZIP")


    # historical_data_2018.zip
    #                  ↓
    #                2018

    if [[ "$YEAR_ZIP_NAME" =~ historical_data_(20[0-9]{2})\.zip ]]; then

        YEAR="${BASH_REMATCH[1]}"

    else

        echo "Cannot identify year:"
        echo "$YEAR_ZIP_NAME"
        continue

    fi


    echo
    echo "============================================================"
    echo "YEAR: $YEAR"
    echo "============================================================"

    echo "Processing:"
    echo "  $YEAR_ZIP_NAME"


    # --------------------------------------------------------
    # 年度临时目录
    # --------------------------------------------------------

    YEAR_TMP="$TMP_ROOT/$YEAR"

    rm -rf "$YEAR_TMP"
    mkdir -p "$YEAR_TMP"


    # --------------------------------------------------------
    # 解开第一层 ZIP
    #
    # historical_data_2018.zip
    #           ↓
    # 4 quarterly ZIPs
    # --------------------------------------------------------

    echo
    echo "Extracting yearly ZIP..."

    unzip -q "$YEAR_ZIP" -d "$YEAR_TMP"


    # --------------------------------------------------------
    # 看看有几个季度 ZIP
    # --------------------------------------------------------

    QZIP_COUNT=$(find "$YEAR_TMP" \
        -type f \
        -name "historical_data_${YEAR}Q[1-4].zip" \
        | wc -l \
        | tr -d ' ')

    echo "Quarter ZIPs found: $QZIP_COUNT"


    if [ "$QZIP_COUNT" -ne 4 ]; then

        echo
        echo "WARNING:"
        echo "Expected 4 quarterly ZIP files for $YEAR,"
        echo "but found $QZIP_COUNT."
        echo

        find "$YEAR_TMP" -type f

    fi


    # ========================================================
    # 5. 第二层：遍历季度 ZIP
    # ========================================================

    while IFS= read -r QUARTER_ZIP
    do

        QUARTER_ZIP_NAME=$(basename "$QUARTER_ZIP")


        # historical_data_2018Q1.zip
        #                       ↓
        #                       1

        if [[ "$QUARTER_ZIP_NAME" =~ ${YEAR}Q([1-4]) ]]; then

            QNUM="${BASH_REMATCH[1]}"
            QUARTER="Q${QNUM}"
            VINTAGE="${YEAR}${QUARTER}"

        else

            echo "Cannot identify quarter:"
            echo "$QUARTER_ZIP_NAME"
            continue

        fi


        echo
        echo "------------------------------------------------------------"
        echo "Processing $VINTAGE"
        echo "------------------------------------------------------------"


        # ----------------------------------------------------
        # 最终目录
        # ----------------------------------------------------

        TARGET_DIR="$OUTPUT_DIR/$YEAR/$QUARTER"

        mkdir -p "$TARGET_DIR"


        ORIG_TARGET="$TARGET_DIR/orig_${VINTAGE}.txt"
        PERF_TARGET="$TARGET_DIR/perf_${VINTAGE}.txt"


        # ----------------------------------------------------
        # 如果已经生成过，直接跳过
        # ----------------------------------------------------

        if [ -f "$ORIG_TARGET" ] && \
           [ -f "$PERF_TARGET" ]; then

            echo "Already extracted. Skipping."

            continue

        fi


        # ----------------------------------------------------
        # 当前季度临时目录
        # ----------------------------------------------------

        QUARTER_TMP="$YEAR_TMP/extracted_${VINTAGE}"

        rm -rf "$QUARTER_TMP"
        mkdir -p "$QUARTER_TMP"


        # ----------------------------------------------------
        # 解开第二层 ZIP
        #
        # historical_data_2018Q1.zip
        #             ↓
        # orig TXT + perf TXT
        # ----------------------------------------------------

        echo "Extracting quarter ZIP..."

        unzip -q "$QUARTER_ZIP" -d "$QUARTER_TMP"


        # ----------------------------------------------------
        # 找 TXT
        # ----------------------------------------------------

        TXT_COUNT=$(find "$QUARTER_TMP" \
            -type f \
            -iname "*.txt" \
            | wc -l \
            | tr -d ' ')

        echo "TXT files found: $TXT_COUNT"


        if [ "$TXT_COUNT" -lt 2 ]; then

            echo
            echo "ERROR: Expected at least 2 TXT files."
            echo

            find "$QUARTER_TMP" -type f

            exit 1

        fi


        # ----------------------------------------------------
        # 自动识别 orig / perf
        # ----------------------------------------------------

        ORIG_FILE=""
        PERF_FILE=""


        while IFS= read -r TXT_FILE
        do

            TXT_NAME=$(basename "$TXT_FILE")

            TXT_LOWER=$(echo "$TXT_NAME" \
                | tr '[:upper:]' '[:lower:]')


            # ------------------------------------------------
            # Performance
            #
            # 常见：
            # historical_data_time_2018Q1.txt
            # ------------------------------------------------

            if [[ "$TXT_LOWER" == *"time"* ]] || \
               [[ "$TXT_LOWER" == *"perf"* ]] || \
               [[ "$TXT_LOWER" == *"svcg"* ]]; then

                PERF_FILE="$TXT_FILE"


            # ------------------------------------------------
            # Origination
            #
            # 有些版本可能显式写 orig
            # ------------------------------------------------

            elif [[ "$TXT_LOWER" == *"orig"* ]]; then

                ORIG_FILE="$TXT_FILE"

            fi


        done < <(
            find "$QUARTER_TMP" \
                -type f \
                -iname "*.txt"
        )


        # ----------------------------------------------------
        # Freddie Mac Standard Dataset 的 orig 常见文件名：
        #
        # historical_data_2018Q1.txt
        #
        # 本身没有 "orig"
        #
        # 所以取剩下那个非-performance TXT
        # ----------------------------------------------------

        if [ -z "$ORIG_FILE" ]; then

            while IFS= read -r TXT_FILE
            do

                if [ "$TXT_FILE" != "$PERF_FILE" ]; then

                    ORIG_FILE="$TXT_FILE"

                    break

                fi

            done < <(
                find "$QUARTER_TMP" \
                    -type f \
                    -iname "*.txt"
            )

        fi


        # ----------------------------------------------------
        # Validation
        # ----------------------------------------------------

        if [ -z "$ORIG_FILE" ]; then

            echo "ERROR: Could not identify Origination TXT."

            find "$QUARTER_TMP" -type f

            exit 1

        fi


        if [ -z "$PERF_FILE" ]; then

            echo "ERROR: Could not identify Performance TXT."

            find "$QUARTER_TMP" -type f

            exit 1

        fi


        echo
        echo "Orig:"
        echo "  $(basename "$ORIG_FILE")"

        echo "Perf:"
        echo "  $(basename "$PERF_FILE")"


        # ----------------------------------------------------
        # 移动并统一命名
        # ----------------------------------------------------

        mv "$ORIG_FILE" "$ORIG_TARGET"
        mv "$PERF_FILE" "$PERF_TARGET"


        echo
        echo "Saved:"
        echo "  $ORIG_TARGET"
        echo "  $PERF_TARGET"


        echo
        echo "Sizes:"

        du -h "$ORIG_TARGET"
        du -h "$PERF_TARGET"


        QUARTER_COUNT=$((QUARTER_COUNT + 1))


        # ----------------------------------------------------
        # 当前季度临时 TXT 删除
        #
        # 最终 TXT 已经 mv 到 OUTPUT_DIR
        # ----------------------------------------------------

        rm -rf "$QUARTER_TMP"


    done < <(
        find "$YEAR_TMP" \
            -type f \
            -name "historical_data_${YEAR}Q[1-4].zip" \
            | sort
    )


    YEAR_COUNT=$((YEAR_COUNT + 1))


    # --------------------------------------------------------
    # 这个年度的临时 quarter ZIP 也不需要了
    # --------------------------------------------------------

    rm -rf "$YEAR_TMP"


done < <(
    find "$ZIP_DIR" \
        -type f \
        -name "historical_data_20??.zip" \
        | sort
)


# ============================================================
# 6. Final Summary
# ============================================================

echo
echo "============================================================"
echo "FINISHED"
echo "============================================================"

echo
echo "Years processed:    $YEAR_COUNT"
echo "Quarters processed: $QUARTER_COUNT"

echo
echo "Total TXT files:"

find "$OUTPUT_DIR" \
    -type f \
    -name "*.txt" \
    | wc -l


echo
echo "TXT storage:"

du -sh "$OUTPUT_DIR"


echo
echo "Original ZIP storage:"

du -sh "$ZIP_DIR"


echo
echo "Output files:"

find "$OUTPUT_DIR" \
    -type f \
    -name "*.txt" \
    | sort
