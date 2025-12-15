#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Run benchmarks against multiple manual configurations to demonstrate auto-tuner effectiveness
# Tests auto-tune against intentionally suboptimal, good, and over-provisioned manual configs

set -euo pipefail

# Configuration
RUNS=${RUNS:-5}
COOLDOWN=${COOLDOWN:-90}
SOURCE_URL=${SOURCE_URL:-""}
TARGET_URL=${TARGET_URL:-""}
OUTPUT_DIR=${OUTPUT_DIR:-"./benchmark_results"}
DATASET=${DATASET:-"imdb"}
TABLES=${TABLES:-"*"}  # snapshot all tables

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

log_header() {
    echo -e "${CYAN}╔════════════════════════════════════════════════════════╗${NC}" >&2
    echo -e "${CYAN}║${NC} $1${CYAN}║${NC}" >&2
    echo -e "${CYAN}╚════════════════════════════════════════════════════════╝${NC}" >&2
}

# Predefined test configurations
# Format: "bytes:description"
# Optimized set covering key failure modes: too small, reasonable, borderline large, over-provisioned
declare -a CONFIGS=(
    "524288:Very Small (512 KB - Too small, high batching overhead)"
    "10485760:Medium (10 MB - Reasonable baseline for comparison)"
    "52428800:Large (50 MB - Approaching memory pressure threshold)"
    "104857600:Very Large (100 MB - Over-provisioned, queue blocking)"
)

check_requirements() {
    if [[ ! -f "./run_benchmark.sh" ]]; then
        echo "Error: run_benchmark.sh not found in current directory" >&2
        echo "Please run this script from tools/benchmark/" >&2
        exit 1
    fi

    if [[ ! -f "./pgstream" ]]; then
        echo "Error: pgstream binary not found in current directory" >&2
        echo "Run 'make build' from repository root first" >&2
        exit 1
    fi

    if [[ -z "$SOURCE_URL" ]]; then
        echo "Error: SOURCE_URL environment variable must be set" >&2
        exit 1
    fi

    if [[ -z "$TARGET_URL" ]]; then
        echo "Error: TARGET_URL environment variable must be set" >&2
        exit 1
    fi
}

setup_master_dir() {
    mkdir -p "$OUTPUT_DIR"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local master_dir="$OUTPUT_DIR/multi_config_${timestamp}"
    mkdir -p "$master_dir"
    echo "$master_dir"
}

run_config_benchmark() {
    local config=$1
    local master_dir=$2
    local config_num=$3
    local total_configs=$4

    local bytes=$(echo "$config" | cut -d: -f1)
    local description=$(echo "$config" | cut -d: -f2-)

    log_header "Configuration $config_num/$total_configs: $description"
    log_info "Batch size: $bytes bytes"
    log_info "Running $RUNS paired tests (auto-tune vs manual)..."

    # Run benchmark with this manual configuration
    RUNS="$RUNS" \
    COOLDOWN="$COOLDOWN" \
    SOURCE_URL="$SOURCE_URL" \
    TARGET_URL="$TARGET_URL" \
    MANUAL_BATCH_BYTES="$bytes" \
    DATASET="$DATASET" \
    TABLES="$TABLES" \
    OUTPUT_DIR="$master_dir/config_${config_num}_${bytes}b" \
    ./run_benchmark.sh

    log_info "✓ Configuration $config_num complete"
    echo "" >&2
}

generate_comparison_report() {
    local master_dir=$1

    log_header "Generating Multi-Configuration Comparison Report"

    local report_file="$master_dir/comparison_report.txt"
    local csv_file="$master_dir/comparison_data.csv"

    # CSV header
    echo "Config,Bytes,Auto_Avg_Throughput_MBps,Manual_Avg_Throughput_MBps,Auto_Avg_Duration_s,Manual_Avg_Duration_s,Throughput_Improvement_%,Duration_Improvement_%,Combined_Improvement_%,Auto_Final_Batch_Bytes,Convergence_Iterations" > "$csv_file"

    {
        echo "╔════════════════════════════════════════════════════════════════════════════════╗"
        echo "║                   MULTI-CONFIGURATION BENCHMARK COMPARISON                     ║"
        echo "╚════════════════════════════════════════════════════════════════════════════════╝"
        echo ""
        echo "Generated: $(date)"
        echo "Runs per configuration: $RUNS"
        echo ""

        # Process each configuration
        local config_num=1
        for config in "${CONFIGS[@]}"; do
            local bytes=$(echo "$config" | cut -d: -f1)
            local description=$(echo "$config" | cut -d: -f2-)
            local config_dir="$master_dir/config_${config_num}_${bytes}b/benchmark_"*

            if [[ -d $config_dir ]]; then
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                echo "Configuration $config_num: $description"
                echo "Manual Batch Size: $bytes bytes ($(numfmt --to=iec-i --suffix=B $bytes 2>/dev/null || echo "$bytes B"))"
                echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                # Extract metrics from all runs
                local auto_throughputs=()
                local manual_throughputs=()
                local auto_durations=()
                local manual_durations=()
                local auto_batch_bytes=""
                local convergence_iters=""

                for run in $(seq 1 $RUNS); do
                    local auto_json="$config_dir/run_${run}_auto.json"
                    local manual_json="$config_dir/run_${run}_manual.json"

                    if [[ -f "$auto_json" ]]; then
                        local throughput=$(grep -o '"throughput_mbps": [0-9.]*' "$auto_json" | awk '{print $2}')
                        # Parse data_snapshot_duration (e.g., "27s" or "2m52s")
                        local duration_str=$(grep -o '"data_snapshot_duration": "[^"]*"' "$auto_json" | cut -d'"' -f4)
                        local duration=0
                        if [[ -n "$duration_str" ]]; then
                            # Extract minutes if present
                            local minutes=$(echo "$duration_str" | grep -o '[0-9]*m' | tr -d 'm' || echo "0")
                            # Extract seconds
                            local seconds=$(echo "$duration_str" | grep -o '[0-9]*s' | tr -d 's' || echo "0")
                            duration=$((minutes * 60 + seconds))
                        fi
                        [[ -n "$throughput" ]] && auto_throughputs+=("$throughput")
                        [[ $duration -gt 0 ]] && auto_durations+=("$duration")

                        # Get auto-tune details from first run
                        if [[ -z "$auto_batch_bytes" ]]; then
                            auto_batch_bytes=$(grep -o '"final_batch_bytes": [0-9]*' "$auto_json" | awk '{print $2}')
                            convergence_iters=$(grep -o '"convergence_iterations": [0-9]*' "$auto_json" | awk '{print $2}')
                        fi
                    fi

                    if [[ -f "$manual_json" ]]; then
                        local throughput=$(grep -o '"throughput_mbps": [0-9.]*' "$manual_json" | awk '{print $2}')
                        # Parse data_snapshot_duration
                        local duration_str=$(grep -o '"data_snapshot_duration": "[^"]*"' "$manual_json" | cut -d'"' -f4)
                        local duration=0
                        if [[ -n "$duration_str" ]]; then
                            local minutes=$(echo "$duration_str" | grep -o '[0-9]*m' | tr -d 'm' || echo "0")
                            local seconds=$(echo "$duration_str" | grep -o '[0-9]*s' | tr -d 's' || echo "0")
                            duration=$((minutes * 60 + seconds))
                        fi
                        [[ -n "$throughput" ]] && manual_throughputs+=("$throughput")
                        [[ $duration -gt 0 ]] && manual_durations+=("$duration")
                    fi
                done

                # Calculate averages
                if [[ ${#auto_throughputs[@]} -gt 0 ]] && [[ ${#manual_throughputs[@]} -gt 0 ]]; then
                    local auto_avg_throughput=$(echo "${auto_throughputs[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')
                    local manual_avg_throughput=$(echo "${manual_throughputs[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')
                    local auto_avg_duration=$(echo "${auto_durations[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')
                    local manual_avg_duration=$(echo "${manual_durations[@]}" | awk '{sum=0; for(i=1;i<=NF;i++) sum+=$i; print sum/NF}')

                    # Calculate improvements
                    local throughput_improvement=$(echo "$auto_avg_throughput $manual_avg_throughput" | awk '{printf "%.2f", ($1-$2)/$2*100}')
                    local duration_improvement=$(echo "$auto_avg_duration $manual_avg_duration" | awk '{printf "%.2f", ($2-$1)/$2*100}')
                    local combined_improvement=$(echo "$throughput_improvement $duration_improvement" | awk '{printf "%.2f", ($1+$2)/2}')

                    echo ""
                    echo "Auto-Tune Results:"
                    echo "  ├─ Average Throughput: ${auto_avg_throughput} MB/s"
                    echo "  ├─ Average Duration: ${auto_avg_duration}s"
                    echo "  ├─ Selected Batch Size: ${auto_batch_bytes} bytes ($(numfmt --to=iec-i --suffix=B $auto_batch_bytes 2>/dev/null || echo "${auto_batch_bytes} B"))"
                    echo "  └─ Convergence Iterations: ${convergence_iters}"
                    echo ""
                    echo "Manual Config Results:"
                    echo "  ├─ Average Throughput: ${manual_avg_throughput} MB/s"
                    echo "  └─ Average Duration: ${manual_avg_duration}s"
                    echo ""
                    echo "Performance Comparison:"

                    # Color-code improvements
                    if (( $(echo "$throughput_improvement > 0" | bc -l) )); then
                        echo "  ├─ Throughput: +${throughput_improvement}% (auto-tune FASTER)"
                    elif (( $(echo "$throughput_improvement < 0" | bc -l) )); then
                        echo "  ├─ Throughput: ${throughput_improvement}% (manual FASTER)"
                    else
                        echo "  ├─ Throughput: ${throughput_improvement}% (EQUAL)"
                    fi

                    if (( $(echo "$duration_improvement > 0" | bc -l) )); then
                        echo "  ├─ Duration: +${duration_improvement}% (auto-tune FASTER)"
                    elif (( $(echo "$duration_improvement < 0" | bc -l) )); then
                        echo "  ├─ Duration: ${duration_improvement}% (manual FASTER)"
                    else
                        echo "  ├─ Duration: ${duration_improvement}% (EQUAL)"
                    fi

                    # Combined improvement
                    if (( $(echo "$combined_improvement > 10" | bc -l) )); then
                        echo "  └─ Combined: +${combined_improvement}% (SIGNIFICANT IMPROVEMENT)"
                    elif (( $(echo "$combined_improvement > 0" | bc -l) )); then
                        echo "  └─ Combined: +${combined_improvement}% (auto-tune better)"
                    elif (( $(echo "$combined_improvement > -5" | bc -l) )); then
                        echo "  └─ Combined: ${combined_improvement}% (roughly equal)"
                    else
                        echo "  └─ Combined: ${combined_improvement}% (manual better)"
                    fi

                    # Add to CSV
                    echo "${description},${bytes},${auto_avg_throughput},${manual_avg_throughput},${auto_avg_duration},${manual_avg_duration},${throughput_improvement},${duration_improvement},${combined_improvement},${auto_batch_bytes},${convergence_iters}" >> "$csv_file"
                else
                    echo "⚠️  Insufficient data for comparison"
                fi
                echo ""
            fi

            config_num=$((config_num + 1))
        done

        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "Summary:"
        echo "  • Results directory: $master_dir"
        echo "  • Comparison CSV: $csv_file"
        echo "  • Individual analysis: See config_*/benchmark_*/analysis_report.txt"
        echo ""
        echo "Key Insights:"
        echo "  1. Very small batches (< 5 MB): High overhead, poor throughput"
        echo "  2. Optimal range: Auto-tuner should converge to efficient batch size"
        echo "  3. Over-provisioned (> 100 MB): Wasteful memory, minimal benefit"
        echo "  4. Auto-tuner advantage: Adapts to network conditions and data characteristics"
        echo ""

    } | tee "$report_file"

    log_info "✓ Comparison report saved to: $report_file"
    log_info "✓ CSV data saved to: $csv_file"
}

main() {
    check_requirements

    local master_dir=$(setup_master_dir)

    log_header "Multi-Configuration Benchmark Suite"
    log_info "Testing ${#CONFIGS[@]} manual configurations vs auto-tune"
    log_info "Master output directory: $master_dir"
    log_info "Runs per configuration: $RUNS"
    log_info "Cooldown between runs: ${COOLDOWN}s"
    log_info "Dataset: $DATASET"
    log_info "Tables: $TABLES"
    echo "" >&2

    # Save configuration
    cat > "$master_dir/benchmark_config.txt" <<EOF
Multi-Configuration Benchmark
=============================
Date: $(date)
Runs per config: $RUNS
Cooldown: ${COOLDOWN}s
Dataset: $DATASET
Tables: $TABLES
Source: $SOURCE_URL
Target: $TARGET_URL

Configurations Tested:
$(printf '%s\n' "${CONFIGS[@]}")
EOF

    # Run benchmark for each configuration
    local config_num=1
    for config in "${CONFIGS[@]}"; do
        run_config_benchmark "$config" "$master_dir" "$config_num" "${#CONFIGS[@]}"
        config_num=$((config_num + 1))
    done

    log_header "All Benchmarks Complete!"

    # Generate comparison report
    generate_comparison_report "$master_dir"

    echo "" >&2
    log_info "╔════════════════════════════════════════════════════════╗"
    log_info "║  Multi-configuration benchmark complete!               ║"
    log_info "╚════════════════════════════════════════════════════════╝"
    log_info ""
    log_info "Next steps:"
    log_info "  1. Review: $master_dir/comparison_report.txt"
    log_info "  2. Analyze CSV: $master_dir/comparison_data.csv"
    log_info "  3. Check individual results in config_* subdirectories"
}

main
