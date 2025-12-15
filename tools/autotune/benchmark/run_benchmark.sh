#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Automated benchmark runner for auto-tuning vs manual configuration
# Uses paired testing to control for network variability

set -euo pipefail

# Configuration
RUNS=${RUNS:-10}
OUTPUT_DIR=${OUTPUT_DIR:-"./benchmark_results"}
COOLDOWN=${COOLDOWN:-180}  # seconds between runs
DATASET=${DATASET:-"imdb"}
TABLES=${TABLES:-"*"}  # snapshot all tables

# pgstream configuration
SOURCE_URL=${SOURCE_URL:-""}
TARGET_URL=${TARGET_URL:-""}
MANUAL_BATCH_BYTES=${MANUAL_BATCH_BYTES:-80000000}  # 80MB default

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1" >&2
}

check_requirements() {
    if [[ ! -f "./pgstream" ]]; then
        log_error "pgstream binary not found in current directory"
        log_error "Run 'make build' first"
        exit 1
    fi

    if [[ -z "$SOURCE_URL" ]]; then
        log_error "SOURCE_URL environment variable must be set"
        exit 1
    fi

    if [[ -z "$TARGET_URL" ]]; then
        log_error "TARGET_URL environment variable must be set"
        exit 1
    fi
}

setup_output_dir() {
    mkdir -p "$OUTPUT_DIR"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local run_dir="$OUTPUT_DIR/benchmark_${timestamp}"
    mkdir -p "$run_dir"
    echo "$run_dir"
}

run_snapshot_test() {
    local config_type=$1  # "auto" or "manual"
    local run_number=$2
    local output_file=$3
    local result_file=$4  # File to write duration:exit_code result

    log_step "Running $config_type configuration - Run $run_number"
    log_info "Source: $SOURCE_URL"
    log_info "Target: $TARGET_URL"

    # Validate URLs are set
    if [[ -z "$SOURCE_URL" ]]; then
        log_error "SOURCE_URL is not set"
        echo "0:1" > "$result_file"
        return 1
    fi
    if [[ -z "$TARGET_URL" ]]; then
        log_error "TARGET_URL is not set"
        echo "0:1" > "$result_file"
        return 1
    fi

    local start_time=$(date +%s)
    local exit_code=0

    if [[ "$config_type" == "auto" ]]; then
        log_info "Config: Auto-tune (min: 1MB, max: 50MB, threshold: 1%)"
        PGSTREAM_POSTGRES_LISTENER_URL="$SOURCE_URL" \
        PGSTREAM_POSTGRES_SNAPSHOT_TABLES="$TABLES" \
        PGSTREAM_POSTGRES_SNAPSHOT_MODE=full \
		PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB=true \
        PGSTREAM_POSTGRES_WRITER_TARGET_URL="$TARGET_URL" \
        PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_ENABLE=true \
        PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_MIN_BYTES=1048576 \
        PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_MAX_BYTES=52428800 \
        PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_CONVERGENCE_THRESHOLD=0.01 \
        PGSTREAM_LOG_LEVEL=debug \
        ./pgstream snapshot \
            >> "$output_file" 2>&1
        exit_code=$?
    else
        log_info "Config: Manual (batch size: $MANUAL_BATCH_BYTES bytes)"
        PGSTREAM_POSTGRES_LISTENER_URL="$SOURCE_URL" \
        PGSTREAM_POSTGRES_SNAPSHOT_MODE=full \
		PGSTREAM_POSTGRES_SNAPSHOT_TABLES="$TABLES" \
		PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB=true \
        PGSTREAM_POSTGRES_WRITER_TARGET_URL="$TARGET_URL" \
        PGSTREAM_POSTGRES_WRITER_BATCH_AUTO_TUNE_ENABLE=false \
        PGSTREAM_POSTGRES_WRITER_BATCH_BYTES="$MANUAL_BATCH_BYTES" \
        PGSTREAM_LOG_LEVEL=debug \
        ./pgstream snapshot \
            >> "$output_file" 2>&1
        exit_code=$?
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    if [[ $exit_code -ne 0 ]]; then
        log_error "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        log_error "SNAPSHOT FAILED"
        log_error "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        log_error "Exit code: $exit_code"
        log_error "Duration: ${duration}s"
        log_error "Config type: $config_type"
        log_error "Log file: $output_file"
        log_error ""
        log_error "Last 30 lines from log:"
        log_error "────────────────────────────────────────"
        tail -30 "$output_file" | sed 's/^/  /' >&2
        log_error "────────────────────────────────────────"
        log_error ""
        log_error "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    else
        log_info "✓ Snapshot completed successfully in ${duration}s"
    fi

    # Write duration and exit code to result file
    echo "${duration}:${exit_code}" > "$result_file"
}

extract_metrics() {
    local log_file=$1
    local metrics_file=$2
    local config_type=$3
    local run_number=$4
    local duration=$5

    # Extract final summary line and strip ANSI escape codes
    # Try multiple patterns to find the summary line
    local summary=$(grep -E "Snapshotting data.*100%|Snapshotting schema.*completed" "$log_file" | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | tr -d '\r' || echo "")

    # If still empty, try to find any line with MB/s
    if [[ -z "$summary" ]]; then
        summary=$(grep "MB/s" "$log_file" | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | tr -d '\r' || echo "")
    fi

    # Warn only if no summary found
    if [[ -z "$summary" ]]; then
        log_warn "Could not find snapshot summary line in log" >&2
    fi

    # Parse throughput (MB/s, kB/s, or GB/s), total size, and duration using awk (macOS compatible)
    # Example: [public] Snapshotting data... 100% [====================] (5.5/5.5 GB, 32 MB/s) [2m52s]
    # Example: [public] Snapshotting data... 100% [====================] (2.1/2.1 GB, 422 kB/s) [1h21m51s]
    local throughput_raw=$(echo "$summary" | awk '{for(i=1;i<=NF;i++){if($i~/[kMG]B\/s/){print $(i-1) " " $i; exit}}}' || echo "0 MB/s")
    local throughput_value=$(echo "$throughput_raw" | awk '{print $1}')
    local throughput_unit=$(echo "$throughput_raw" | awk '{print $2}')

    # Convert to MB/s for consistency
    local throughput=0
    if [[ "$throughput_unit" == "GB/s" ]]; then
        throughput=$(echo "$throughput_value * 1024" | bc)
    elif [[ "$throughput_unit" == "MB/s" ]]; then
        throughput=$throughput_value
    elif [[ "$throughput_unit" == "kB/s" ]]; then
        throughput=$(echo "$throughput_value / 1024" | bc -l)
    fi

    # Extract size and unit from parentheses: (5.5/5.5 GB, ...)
    local size_part=$(echo "$summary" | awk -F'[()]' '{print $2}' | awk '{print $1}')
    local total_size=$(echo "$size_part" | awk -F'/' '{print $2}')
    local size_unit=$(echo "$summary" | awk -F'[()]' '{print $2}' | awk '{print $2}' | tr -d ',')

    # Extract duration from last brackets: [2m52s] (already cleaned above)
    local data_snapshot_duration=$(echo "$summary" | awk -F'[][]' '{print $(NF-1)}' || echo "")

    # Convert to bytes
    local total_bytes=0
    if [[ -n "$total_size" ]] && [[ "$total_size" != "0" ]]; then
        if [[ "$size_unit" == "GB" ]]; then
            total_bytes=$(echo "$total_size * 1024 * 1024 * 1024" | bc | cut -d. -f1)
        elif [[ "$size_unit" == "MB" ]]; then
            total_bytes=$(echo "$total_size * 1024 * 1024" | bc | cut -d. -f1)
        elif [[ "$size_unit" == "KB" ]]; then
            total_bytes=$(echo "$total_size * 1024" | bc | cut -d. -f1)
        fi
    fi

    # Extract auto-tune specific metrics
    local convergence_iterations=0
    local final_batch_bytes=0
    local skipped_batches=0

    if [[ "$config_type" == "auto" ]]; then
        convergence_iterations=$(grep -c "batch bytes tuner has converged" "$log_file" || true)
        skipped_batches=$(grep -c "skipping measurement" "$log_file" || true)

        # Extract final batch bytes from the line after "final candidate:"
        # Format: "final candidate:" on one line, then "[value: 46407680, ..." on the next
        final_batch_bytes=$(grep -A 1 "^final candidate:" "$log_file" | tail -1 | sed -n 's/.*\[value: \([0-9]*\),.*/\1/p' || echo "0")

        # Fallback: try to find it on the same line if the above fails
        if [[ "$final_batch_bytes" == "0" ]]; then
            final_batch_bytes=$(grep "final candidate:" "$log_file" | tail -1 | sed -n 's/.*\[value: \([0-9]*\),.*/\1/p' || echo "0")
        fi
    fi

    # Create JSON output (escape data_snapshot_duration for JSON safety)
    local data_snapshot_duration_json="${data_snapshot_duration//\\/\\\\}"
    data_snapshot_duration_json="${data_snapshot_duration_json//\"/\\\"}"

    cat > "$metrics_file" <<EOF
{
  "run_number": $run_number,
  "config_type": "$config_type",
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "duration_sec": $duration,
  "data_snapshot_duration": "${data_snapshot_duration_json}",
  "total_bytes": ${total_bytes:-0},
  "throughput_mbps": ${throughput:-0},
  "dataset": "$DATASET",
  "manual_batch_bytes": $MANUAL_BATCH_BYTES,
  "convergence_iterations": $convergence_iterations,
  "final_batch_bytes": ${final_batch_bytes:-0},
  "skipped_batches": $skipped_batches,
  "log_file": "$log_file"
}
EOF
}

run_paired_test() {
    local run_number=$1
    local run_dir=$2

    log_info "═══════════════════════════════════════"
    log_info "Starting paired test run $run_number/$RUNS"
    log_info "═══════════════════════════════════════"

    # Run auto-tune first
    local auto_log="$run_dir/run_${run_number}_auto.log"
    local auto_metrics="$run_dir/run_${run_number}_auto.json"
    local auto_result_file="$run_dir/run_${run_number}_auto.result"
    run_snapshot_test "auto" "$run_number" "$auto_log" "$auto_result_file"
    local auto_result=$(cat "$auto_result_file")
    local auto_duration=$(echo "$auto_result" | cut -d: -f1)
    local auto_exit_code=$(echo "$auto_result" | cut -d: -f2)

    # Check if snapshot failed
    if [[ $auto_exit_code -ne 0 ]]; then
        log_error "⚠️  Auto-tune test failed, skipping remaining tests"
        return 1
    fi

    extract_metrics "$auto_log" "$auto_metrics" "auto" "$run_number" "$auto_duration"

    log_info "Cooling down for ${COOLDOWN}s..."
    sleep "$COOLDOWN"

    # Run manual configuration
    local manual_log="$run_dir/run_${run_number}_manual.log"
    local manual_metrics="$run_dir/run_${run_number}_manual.json"
    local manual_result_file="$run_dir/run_${run_number}_manual.result"
    run_snapshot_test "manual" "$run_number" "$manual_log" "$manual_result_file"
    local manual_result=$(cat "$manual_result_file")
    local manual_duration=$(echo "$manual_result" | cut -d: -f1)
    local manual_exit_code=$(echo "$manual_result" | cut -d: -f2)

    # Check if snapshot failed
    if [[ $manual_exit_code -ne 0 ]]; then
        log_error "⚠️  Manual test failed"
        return 1
    fi

    extract_metrics "$manual_log" "$manual_metrics" "manual" "$run_number" "$manual_duration"

    if [[ $run_number -lt $RUNS ]]; then
        log_info "Cooling down for ${COOLDOWN}s before next pair..."
        sleep "$COOLDOWN"
    fi
}

generate_summary() {
    local run_dir=$1

    log_info "Generating benchmark summary..."

    if command -v python3 &> /dev/null; then
        python3 "$(dirname "$0")/analyze_results.py" "$run_dir"
    else
        log_warn "Python3 not found, skipping statistical analysis"
        log_info "Run manually: python3 tools/benchmark/analyze_results.py $run_dir"
    fi
}

main() {
    check_requirements
    local run_dir=$(setup_output_dir)

    log_info "Starting benchmark suite"
    log_info "Output directory: $run_dir"
    log_info "Total runs: $RUNS pairs (${RUNS} auto + ${RUNS} manual)"
    log_info "Dataset: $DATASET"
    log_info "Manual batch size: $MANUAL_BATCH_BYTES bytes"
    log_info ""

    # Save configuration
    cat > "$run_dir/config.txt" <<EOF
Benchmark Configuration
=======================
Date: $(date)
Runs: $RUNS
Cooldown: ${COOLDOWN}s
Dataset: $DATASET
Source: $SOURCE_URL
Target: $TARGET_URL
Manual Batch Bytes: $MANUAL_BATCH_BYTES
EOF

    # Run paired tests
    for run in $(seq 1 $RUNS); do
        run_paired_test "$run" "$run_dir"
    done

    log_info "═══════════════════════════════════════"
    log_info "Benchmark complete!"
    log_info "═══════════════════════════════════════"
    log_info "Results saved to: $run_dir"

    generate_summary "$run_dir"
}

main
