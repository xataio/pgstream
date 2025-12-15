#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Network baseline characterization script
# Measures database connectivity and throughput to understand variability

set -euo pipefail

# Configuration
RUNS=${RUNS:-30}          # number of test runs
OUTPUT_DIR=${OUTPUT_DIR:-"./benchmark_results"}
TARGET_URL=${TARGET_URL:-""}
TEST_SIZE=${TEST_SIZE:-10000}  # Number of rows to test with

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_requirements() {
    if ! command -v psql &> /dev/null; then
        log_error "psql is required but not installed. Install with:"
        log_error "  macOS: brew install postgresql"
        log_error "  Linux: apt-get install postgresql-client"
        exit 1
    fi

    if [[ -z "$TARGET_URL" ]]; then
        log_error "TARGET_URL environment variable must be set to a PostgreSQL connection URL"
        log_error "Usage: TARGET_URL='postgresql://user:pass@host:port/db' ./network_baseline.sh"
        exit 1
    fi
}

setup_output_dir() {
    mkdir -p "$OUTPUT_DIR"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local run_dir="$OUTPUT_DIR/network_baseline_${timestamp}"
    mkdir -p "$run_dir"
    echo "$run_dir"
}

setup_test_table() {
    log_info "Setting up test table..."

    # Test connectivity first
    if ! psql "$TARGET_URL" -c "SELECT 1;" > /dev/null 2>&1; then
        log_error "Cannot connect to database. Please verify:"
        log_error "  1. Database is running"
        log_error "  2. Connection URL is correct"
        log_error "  3. User has necessary permissions"
        log_error "  Connection string: $TARGET_URL"
        exit 1
    fi

    # Create test table and populate with data
    psql "$TARGET_URL" -c "DROP TABLE IF EXISTS pgstream_baseline_test;" 2>/dev/null || true
    if ! psql "$TARGET_URL" -c "CREATE TABLE pgstream_baseline_test (id SERIAL PRIMARY KEY, data TEXT);" > /dev/null 2>&1; then
        log_error "Failed to create test table. Check user permissions."
        exit 1
    fi
    psql "$TARGET_URL" -c "INSERT INTO pgstream_baseline_test (data) SELECT md5(random()::text) FROM generate_series(1, $TEST_SIZE);" > /dev/null

    log_info "Test table created with $TEST_SIZE rows"
}

cleanup_test_table() {
    log_info "Cleaning up test table..."
    psql "$TARGET_URL" -c "DROP TABLE IF EXISTS pgstream_baseline_test;" > /dev/null 2>&1 || true
}

run_baseline_tests() {
    local run_dir=$1
    local results_file="$run_dir/network_baseline.csv"
    local summary_file="$run_dir/summary.txt"

    log_info "Starting network baseline characterization"
    log_info "Target: $TARGET_URL"
    log_info "Runs: $RUNS"
    log_info "Results will be saved to: $run_dir"

    # Setup test table
    setup_test_table

    # CSV header
    echo "run,duration_ms,rows,throughput_rows_per_sec" > "$results_file"

    local success_count=0
    for i in $(seq 1 $RUNS); do
        log_info "Run $i/$RUNS"

        # Use gdate if available (GNU date on macOS), otherwise fall back to seconds
        if command -v gdate &> /dev/null; then
            local start_time=$(gdate +%s%3N)
            local row_count=$(psql "$TARGET_URL" -t -c "SELECT COUNT(*) FROM pgstream_baseline_test;" 2>&1 | tr -d '[:space:]')
            local end_time=$(gdate +%s%3N)
        else
            local start_time=$(date +%s)000
            local row_count=$(psql "$TARGET_URL" -t -c "SELECT COUNT(*) FROM pgstream_baseline_test;" 2>&1 | tr -d '[:space:]')
            local end_time=$(date +%s)000
        fi

        if [[ "$row_count" =~ ^[0-9]+$ ]]; then
            local duration=$((end_time - start_time))
            local throughput=$(echo "scale=2; $row_count * 1000 / $duration" | bc)
            echo "$i,$duration,$row_count,$throughput" >> "$results_file"
            ((success_count++))
        else
            log_warn "Run $i failed, check connectivity to $TARGET_URL"
            echo "$i,0,0,0" >> "$results_file"
        fi

        # Short pause between runs
        if [[ $i -lt $RUNS ]]; then
            sleep 2
        fi
    done

    # Cleanup
    cleanup_test_table

    # Generate summary statistics
    if [[ $success_count -gt 0 ]]; then
        generate_summary "$results_file" "$summary_file"
    else
        log_error "All runs failed. Check database connectivity."
        return 1
    fi

    log_info "Baseline characterization complete ($success_count/$RUNS successful)"
    log_info "Results: $results_file"
    log_info "Summary: $summary_file"
}

generate_summary() {
    local results_file=$1
    local summary_file=$2

    log_info "Generating summary statistics..."

    # Extract throughput values (rows/sec), skip header and failed runs
    local throughputs=$(tail -n +2 "$results_file" | awk -F',' '$4 > 0 {print $4}')
    local durations=$(tail -n +2 "$results_file" | awk -F',' '$2 > 0 {print $2}')

    if [[ -z "$throughputs" ]]; then
        log_warn "No throughput data found in results"
        return
    fi

    # Calculate statistics using awk
    {
        echo "Database Connectivity Baseline Statistics"
        echo "=========================================="
        echo ""
        echo "Target: $TARGET_URL"
        echo "Test size: $TEST_SIZE rows"
        echo ""

        echo "$throughputs" | awk '
        BEGIN {
            sum = 0
            count = 0
            min = 999999999999
            max = 0
        }
        {
            sum += $1
            count++
            if ($1 < min) min = $1
            if ($1 > max) max = $1
            values[count] = $1
        }
        END {
            mean = sum / count

            # Calculate standard deviation
            sum_sq_diff = 0
            for (i = 1; i <= count; i++) {
                diff = values[i] - mean
                sum_sq_diff += diff * diff
            }
            stddev = sqrt(sum_sq_diff / count)
            cov = stddev / mean

            print "Number of successful runs: " count
            print ""
            print "Query Throughput (rows/second):"
            print "  Mean:   " sprintf("%.2f", mean)
            print "  StdDev: " sprintf("%.2f", stddev)
            print "  Min:    " sprintf("%.2f", min)
            print "  Max:    " sprintf("%.2f", max)
            print "  CoV:    " sprintf("%.4f", cov) " (" sprintf("%.1f%%)", cov * 100) ")"
            print ""
        }'

        echo "$durations" | awk '
        BEGIN {
            sum = 0
            count = 0
        }
        {
            sum += $1
            count++
        }
        END {
            mean = sum / count

            print "Query Latency (milliseconds):"
            print "  Mean:   " sprintf("%.0f", mean) " ms"
            print ""
        }'

        echo "$throughputs" | awk '
        BEGIN {
            sum = 0
            count = 0
        }
        {
            sum += $1
            count++
            values[count] = $1
        }
        END {
            mean = sum / count
            sum_sq_diff = 0
            for (i = 1; i <= count; i++) {
                diff = values[i] - mean
                sum_sq_diff += diff * diff
            }
            stddev = sqrt(sum_sq_diff / count)
            cov = stddev / mean

            if (cov < 0.1) {
                print "Connection stability: EXCELLENT (CoV < 10%)"
            } else if (cov < 0.2) {
                print "Connection stability: GOOD (CoV < 20%)"
            } else if (cov < 0.4) {
                print "Connection stability: MODERATE (CoV < 40%)"
            } else {
                print "Connection stability: POOR (CoV >= 40%)"
                print "WARNING: High variability may affect auto-tuning accuracy"
            }
        }'
    } | tee "$summary_file"
}

main() {
    check_requirements
    local run_dir=$(setup_output_dir)
    run_baseline_tests "$run_dir"
}

main
