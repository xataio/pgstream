# Auto-Tuning Benchmark Suite

This directory contains tools for benchmarking auto-tuning performance improvements with proper statistical controls for network variability.

## Overview

The benchmark suite consists of:

1. **Network Baseline** - Characterize network stability before testing
2. **Paired Benchmark Runner** - Run auto-tune vs manual configs with statistical controls
3. **Statistical Analysis** - Analyze results with effect sizes and significance tests
4. **Network Simulation** (Linux only) - Test under controlled network conditions

## Quick Start

### 1. Build pgstream

```bash
cd /Users/esther/go/src/github.com/xataio/pgstream
make build
```

### 2. Characterize Database Connectivity Baseline (Optional but Recommended)

```bash
# Characterize connection stability to your target database
TARGET_URL="postgresql://user:pass@host:port/db" ./tools/benchmark/network_baseline.sh
```

This measures database connectivity and query performance variability (CoV) before running benchmarks.

### 3. Run Benchmark Suite

```bash
# Set your database URLs
export SOURCE_URL="postgresql://user:pass@source:5432/db"
export TARGET_URL="postgresql://user:pass@target:5432/db"

# Run with defaults (10 paired runs)
./tools/benchmark/run_benchmark.sh

# Or customize
RUNS=20 \
MANUAL_BATCH_BYTES=80000000 \
COOLDOWN=300 \
./tools/benchmark/run_benchmark.sh
```

### 4. Analyze Results

Results are automatically analyzed at the end. To re-analyze:

```bash
python3 tools/benchmark/analyze_results.py ./benchmark_results/benchmark_20251209_143022/
```

## Detailed Usage

### Database Connectivity Baseline Characterization

Measures database connection stability and query performance variability:

```bash
# Default: 30 runs with test queries
TARGET_URL="postgresql://user:pass@host:port/db" ./tools/benchmark/network_baseline.sh

# Customize
RUNS=50 \
TEST_SIZE=20000 \
OUTPUT_DIR=./my_baseline \
TARGET_URL="postgresql://user:pass@host:port/db" \
./tools/benchmark/network_baseline.sh
```

**What it does:**

- Creates a temporary test table with specified rows
- Runs multiple query tests to measure latency and throughput
- Calculates coefficient of variation (CoV) to assess stability
- Cleans up test data automatically
- Works on both macOS and Linux (uses `gdate` if available for millisecond precision)

**Output:**

- `network_baseline.csv` - Raw test results (duration, rows, throughput)
- `summary.txt` - Statistics including CoV (coefficient of variation)

**Interpreting CoV:**

- < 10%: Excellent stability
- 10-20%: Good stability
- 20-40%: Moderate stability (auto-tune should still work)
- > 40%: Poor stability (may affect auto-tune accuracy)

### Benchmark Runner

Performs paired testing (alternates between auto-tune and manual):

```bash
# Required environment variables
export SOURCE_URL="postgresql://user:pass@source:5432/db"
export TARGET_URL="postgresql://user:pass@target:5432/db"

# Optional configuration
export RUNS=10                      # Number of paired runs (default: 10)
export COOLDOWN=180                 # Seconds between runs (default: 180)
export MANUAL_BATCH_BYTES=80000000  # Manual config batch size (default: 80MB)
export DATASET="imdb"               # Dataset name for reporting
export OUTPUT_DIR="./benchmark_results"

./tools/benchmark/run_benchmark.sh
```

**Each run performs:**

1. Auto-tune snapshot with full logging
2. Cool-down period (3 minutes default)
3. Manual config snapshot with same batch size
4. Cool-down period before next pair

**Why paired testing?**
Alternating configs controls for time-based network variance. If network degrades over time, both configs are affected equally.

### Statistical Analysis

Automatically runs at end of benchmark, or run manually:

```bash
python3 tools/benchmark/analyze_results.py <results_directory>
```

**Report includes:**

- Throughput statistics (mean, median, std dev, CoV)
- Duration statistics
- Improvement percentages
- Paired t-test results
- Cohen's d effect size
- Auto-tune specific metrics (convergence iterations, skipped batches)
- Stability assessment
- Recommendations

**Interpreting Results:**

- **Improvement %**: Positive means auto-tune is faster
- **Cohen's d**:
  - < 0.2: Negligible effect
  - 0.2-0.5: Small effect
  - 0.5-0.8: Medium effect
  - > 0.8: Large effect
- **CoV**: Lower is better (more consistent)

### Network Condition Simulation (Linux Only)

Test auto-tuning under controlled network conditions:

```bash
# Setup condition (requires sudo)
sudo ./tools/benchmark/setup_network_conditions.sh medium-jitter

# Run benchmark
./tools/benchmark/run_benchmark.sh

# Clean up
sudo ./tools/benchmark/setup_network_conditions.sh clean
```

**Available conditions:**

- `stable` - Minimal jitter (10ms ±2ms)
- `low-jitter` - Low jitter (10ms ±5ms)
- `medium-jitter` - Medium jitter (20ms ±20ms)
- `high-jitter` - High jitter (30ms ±50ms)
- `packet-loss` - 1% packet loss
- `bandwidth` - 100 Mbps limit
- `congestion` - Combined effects

**macOS Users:**
Use [Network Link Conditioner](https://developer.apple.com/download/more/) from Apple's Additional Tools for Xcode.

### Multi-Configuration Testing

Test auto-tuning against multiple manual configurations (including intentionally bad ones):

```bash
# Tests 7 different manual configs: 512KB, 2MB, 10MB, 20MB, 50MB, 100MB, 200MB
SOURCE_URL="..." \
TARGET_URL="..." \
RUNS=5 \
./tools/benchmark/run_multi_config_benchmark.sh
```

**What it does:**

- Tests auto-tune against 7 predefined manual configurations:
  - Very Small (512 KB): Too small, many round trips
  - Small (2 MB): Suboptimal, frequent batching
  - Medium (10 MB): Reasonable baseline
  - Good (20 MB): Well-balanced
  - Large (50 MB): High memory, diminishing returns
  - Very Large (100 MB): Over-provisioned, wasteful
  - Excessive (200 MB): Extremely wasteful
- Runs full paired benchmarks for each configuration
- Generates comprehensive comparison report
- Creates CSV with all results for analysis

**Output:**

- `comparison_report.txt` - Human-readable comparison across all configs
- `comparison_data.csv` - Data for further analysis (Excel, plotting, etc.)
- Individual `config_*/` directories with full benchmark results

**Best for:**

- Demonstrating auto-tuner effectiveness across range of manual configs
- Finding the optimal manual baseline if auto-tune is not available
- Visualizing performance degradation with bad configs
- Proving that auto-tune adapts better than fixed configuration

## Example Workflows

### Basic Performance Test

```bash
# Quick test with defaults
SOURCE_URL="..." TARGET_URL="..." ./tools/benchmark/run_benchmark.sh
```

### Multi-Config Comparison (Recommended)

```bash
# Test against range of manual configs to show auto-tune superiority
SOURCE_URL="..." \
TARGET_URL="..." \
RUNS=5 \
COOLDOWN=90 \
./tools/benchmark/run_multi_config_benchmark.sh

# Review results
cat benchmark_results/multi_config_*/comparison_report.txt
```

### Comprehensive Testing

```bash
# 1. Characterize database connectivity
TARGET_URL="postgresql://user:pass@target:5432/db" ./tools/benchmark/network_baseline.sh

# 2. Run extensive benchmark
RUNS=20 \
COOLDOWN=300 \
SOURCE_URL="..." \
TARGET_URL="..." \
./tools/benchmark/run_benchmark.sh

# 3. Results automatically analyzed
```

### Testing Multiple Network Conditions (Linux)

```bash
#!/bin/bash
for condition in stable medium-jitter high-jitter; do
    echo "Testing under $condition..."
    sudo ./tools/benchmark/setup_network_conditions.sh $condition

    RUNS=10 \
    OUTPUT_DIR="./results_${condition}" \
    SOURCE_URL="..." \
    TARGET_URL="..." \
    ./tools/benchmark/run_benchmark.sh

    sudo ./tools/benchmark/setup_network_conditions.sh clean
    sleep 300
done
```

### Testing Different Manual Batch Sizes

```bash
for batch_size in 40000000 80000000 120000000; do
    RUNS=10 \
    MANUAL_BATCH_BYTES=$batch_size \
    OUTPUT_DIR="./results_${batch_size}" \
    SOURCE_URL="..." \
    TARGET_URL="..." \
    ./tools/benchmark/run_benchmark.sh
done
```

## Output Structure

```
benchmark_results/
└── benchmark_20251209_143022/
    ├── config.txt                    # Test configuration
    ├── run_1_auto.log               # Detailed logs
    ├── run_1_auto.json              # Metrics
    ├── run_1_manual.log
    ├── run_1_manual.json
    ├── run_2_auto.log
    ├── ...
    └── analysis_report.txt          # Statistical analysis
```

## Troubleshooting

### "pgstream binary not found"

```bash
cd /Users/esther/go/src/github.com/xataio/pgstream
make build
```

### "psql not found"

```bash
brew install postgresql  # macOS
apt-get install postgresql-client  # Linux
```

### Network simulation not working on macOS

Use Network Link Conditioner instead. The tc-based simulation only works on Linux.

### Results show high variance

- Check network baseline CoV
- Increase RUNS for more statistical power
- Increase COOLDOWN between runs
- Test during off-peak hours
- Consider testing under simulated stable conditions

### Auto-tune not converging

Check logs for:

- "network too unstable for batch bytes tuning" - Network variance > 40% CoV
- "skipping measurement" - Batch sizes varying too much
- Low convergence iterations - May need more data

## Requirements

- **pgstream** - Built binary in repository root
- **bash** - Shell script execution
- **python3** - Statistical analysis (standard library only)
- **psql** - PostgreSQL client for baseline testing (optional)
- **bc** - Basic calculator for statistics
- **tc/iproute2** - Network simulation on Linux (optional)

## Best Practices

1. **Always run network baseline first** to understand your environment
2. **Use paired testing** - Don't run all auto-tune then all manual
3. **Collect sufficient samples** - Minimum 10 pairs, prefer 20+
4. **Allow cool-down** - 3-5 minutes between runs
5. **Test during stable periods** - Avoid peak usage times
6. **Document conditions** - Network, system load, data size
7. **Verify data integrity** - Check source/target match after tests
8. **Monitor system resources** - Ensure no CPU/memory/disk bottlenecks

## Statistical Notes

- **Paired t-test**: Tests whether mean difference is significantly different from zero
- **Cohen's d**: Measures practical significance (effect size)
- **CoV (Coefficient of Variation)**: Measures consistency (lower = more reliable)
- **Confidence**: With 20 samples, you can detect medium effects (d=0.5) with ~80% power

For production research, consider using scipy for more robust statistical tests:

```bash
pip install scipy numpy
# Modify analyze_results.py to use scipy.stats.ttest_rel
```

## Contributing

When adding new benchmark tools:

1. Follow existing script structure
2. Add comprehensive help text
3. Output structured data (JSON/CSV)
4. Document in this README
5. Add example usage
