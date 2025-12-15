#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Statistical analysis of auto-tuning benchmark results
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Tuple
import statistics
import re


def parse_duration_string(duration_str: str) -> float:
    """Parse duration strings like '27s', '2m52s', '11m1s' to seconds."""
    if not duration_str:
        return 0.0

    total_seconds = 0.0

    # Match minutes
    minutes_match = re.search(r'(\d+)m', duration_str)
    if minutes_match:
        total_seconds += int(minutes_match.group(1)) * 60

    # Match seconds
    seconds_match = re.search(r'(\d+)s', duration_str)
    if seconds_match:
        total_seconds += int(seconds_match.group(1))

    return total_seconds


def load_results(run_dir: Path) -> Tuple[List[Dict], List[Dict]]:
    """Load all auto and manual test results from the run directory."""
    auto_results = []
    manual_results = []

    for json_file in sorted(run_dir.glob("run_*_*.json")):
        with open(json_file, 'r') as f:
            data = json.load(f)
            if data['config_type'] == 'auto':
                auto_results.append(data)
            else:
                manual_results.append(data)

    return auto_results, manual_results


def calculate_statistics(values: List[float]) -> Dict:
    """Calculate descriptive statistics for a list of values."""
    if not values:
        return {}

    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0
    cov = stdev / mean if mean != 0 else 0

    return {
        'mean': mean,
        'median': statistics.median(values),
        'stdev': stdev,
        'min': min(values),
        'max': max(values),
        'cov': cov,
        'count': len(values)
    }


def paired_t_test(auto_values: List[float], manual_values: List[float]) -> Dict:
    """
    Perform paired t-test and calculate effect size.
    Note: This is a simplified implementation. For production, use scipy.stats.ttest_rel
    """
    if len(auto_values) != len(manual_values):
        return {'error': 'Sample sizes do not match'}

    if len(auto_values) < 2:
        return {'error': 'Insufficient samples for t-test'}

    # Calculate differences
    diffs = [a - m for a, m in zip(auto_values, manual_values)]

    mean_diff = statistics.mean(diffs)
    n = len(diffs)

    # Handle case where all differences are identical (no variance)
    if n < 2:
        stdev_diff = 0
    else:
        try:
            stdev_diff = statistics.stdev(diffs)
        except statistics.StatisticsError:
            stdev_diff = 0

    # Calculate t-statistic (handle zero variance case)
    if stdev_diff == 0:
        # If there's no variance, t-stat is undefined or infinite
        # If mean_diff is also 0, there's no difference; otherwise it's significant
        t_stat = float('inf') if mean_diff != 0 else 0
    else:
        t_stat = mean_diff / (stdev_diff / (n ** 0.5))

    # Calculate Cohen's d (effect size)
    try:
        auto_stdev = statistics.stdev(
            auto_values) if len(auto_values) > 1 else 0
        manual_stdev = statistics.stdev(
            manual_values) if len(manual_values) > 1 else 0
        pooled_stdev = ((auto_stdev ** 2 + manual_stdev ** 2) / 2) ** 0.5
        cohen_d = mean_diff / pooled_stdev if pooled_stdev != 0 else 0
    except statistics.StatisticsError:
        cohen_d = 0

    return {
        't_statistic': t_stat,
        'cohen_d': cohen_d,
        'mean_difference': mean_diff,
        'degrees_of_freedom': n - 1
    }


def interpret_effect_size(cohen_d: float) -> str:
    """Interpret Cohen's d effect size."""
    abs_d = abs(cohen_d)
    if abs_d < 0.2:
        return "negligible"
    elif abs_d < 0.5:
        return "small"
    elif abs_d < 0.8:
        return "medium"
    else:
        return "large"


def generate_report(run_dir: Path, auto_results: List[Dict],
                    manual_results: List[Dict]) -> str:
    """Generate a comprehensive analysis report."""

    # Extract throughput values
    auto_throughputs = [r['throughput_mbps'] for r in auto_results
                        if r['throughput_mbps'] > 0]
    manual_throughputs = [r['throughput_mbps'] for r in manual_results
                          if r['throughput_mbps'] > 0]

    # Extract duration values - use data_snapshot_duration if available, fallback to duration_sec
    auto_durations = []
    for r in auto_results:
        if 'data_snapshot_duration' in r and r['data_snapshot_duration']:
            auto_durations.append(parse_duration_string(
                r['data_snapshot_duration']))
        else:
            auto_durations.append(r['duration_sec'])

    manual_durations = []
    for r in manual_results:
        if 'data_snapshot_duration' in r and r['data_snapshot_duration']:
            manual_durations.append(
                parse_duration_string(r['data_snapshot_duration']))
        else:
            manual_durations.append(r['duration_sec'])

    # Calculate statistics
    auto_throughput_stats = calculate_statistics(auto_throughputs)
    manual_throughput_stats = calculate_statistics(manual_throughputs)
    auto_duration_stats = calculate_statistics(auto_durations)
    manual_duration_stats = calculate_statistics(manual_durations)

    # Perform statistical tests
    throughput_test = paired_t_test(auto_throughputs, manual_throughputs)

    # Calculate improvement percentages
    throughput_improvement = 0
    duration_improvement = 0
    if manual_throughput_stats.get('mean', 0) > 0:
        throughput_improvement = (
            (auto_throughput_stats['mean'] - manual_throughput_stats['mean']) /
            manual_throughput_stats['mean'] * 100
        )
    if manual_duration_stats.get('mean', 0) > 0:
        duration_improvement = (
            (manual_duration_stats['mean'] - auto_duration_stats['mean']) /
            manual_duration_stats['mean'] * 100
        )

    # Calculate combined performance improvement
    # This considers both throughput (higher is better) and duration (lower is better)
    # Combined improvement = (throughput_improvement + duration_improvement) / 2
    combined_improvement = (throughput_improvement + duration_improvement) / 2

    # Auto-tune specific metrics
    convergence_data = [r['convergence_iterations'] for r in auto_results]
    skipped_data = [r['skipped_batches'] for r in auto_results]
    final_batch_bytes_data = [r.get('final_batch_bytes', 0)
                              for r in auto_results if r.get('final_batch_bytes', 0) > 0]

    # Calculate manual batch size for comparison
    manual_batch_mb = auto_results[0]['manual_batch_bytes'] / \
        1048576 if auto_results else 0

    # Check if auto-tune consistently selects similar batch sizes to manual
    avg_auto_batch_mb = statistics.mean(
        final_batch_bytes_data) / 1048576 if final_batch_bytes_data else 0
    batch_size_diff_pct = 0
    if manual_batch_mb > 0:
        batch_size_diff_pct = (
            (avg_auto_batch_mb - manual_batch_mb) / manual_batch_mb) * 100

    # Generate report
    report = f"""
{'='*70}
AUTO-TUNING BENCHMARK ANALYSIS
{'='*70}

Configuration:
  Total runs: {len(auto_results)} pairs
  Dataset: {auto_results[0]['dataset'] if auto_results else 'N/A'}
  Manual batch size: {auto_results[0]['manual_batch_bytes'] if auto_results else 'N/A'} bytes

{'='*70}
THROUGHPUT ANALYSIS (MB/s)
{'='*70}

Auto-Tune:
  Mean:   {auto_throughput_stats.get('mean', 0):>8.2f} MB/s
  Median: {auto_throughput_stats.get('median', 0):>8.2f} MB/s
  StdDev: {auto_throughput_stats.get('stdev', 0):>8.2f} MB/s
  Min:    {auto_throughput_stats.get('min', 0):>8.2f} MB/s
  Max:    {auto_throughput_stats.get('max', 0):>8.2f} MB/s
  CoV:    {auto_throughput_stats.get('cov', 0):>8.2%}

Manual Config:
  Mean:   {manual_throughput_stats.get('mean', 0):>8.2f} MB/s
  Median: {manual_throughput_stats.get('median', 0):>8.2f} MB/s
  StdDev: {manual_throughput_stats.get('stdev', 0):>8.2f} MB/s
  Min:    {manual_throughput_stats.get('min', 0):>8.2f} MB/s
  Max:    {manual_throughput_stats.get('max', 0):>8.2f} MB/s
  CoV:    {manual_throughput_stats.get('cov', 0):>8.2%}

Improvement:  {throughput_improvement:>+7.2f}%

{'='*70}
DATA SNAPSHOT DURATION ANALYSIS (seconds)
{'='*70}

Auto-Tune:
  Mean:   {auto_duration_stats.get('mean', 0):>8.0f}s
  Median: {auto_duration_stats.get('median', 0):>8.0f}s
  StdDev: {auto_duration_stats.get('stdev', 0):>8.0f}s

Manual Config:
  Mean:   {manual_duration_stats.get('mean', 0):>8.0f}s
  Median: {manual_duration_stats.get('median', 0):>8.0f}s
  StdDev: {manual_duration_stats.get('stdev', 0):>8.0f}s

Improvement:  {duration_improvement:>+7.2f}%

Note: Using data snapshot duration (excludes setup/teardown overhead)

{'='*70}
OVERALL PERFORMANCE IMPROVEMENT
{'='*70}

Combined Performance Improvement: {combined_improvement:>+7.2f}%
  (Average of throughput and duration improvements)

Breakdown:
  Throughput: {throughput_improvement:>+7.2f}% (higher is better)
  Duration:   {duration_improvement:>+7.2f}% (positive = faster)

{'='*70}
STATISTICAL TESTS
{'='*70}

Paired T-Test (Throughput):
  t-statistic: {throughput_test.get('t_statistic', 0):>8.3f}
  Cohen's d:   {throughput_test.get('cohen_d', 0):>8.3f} ({interpret_effect_size(throughput_test.get('cohen_d', 0))})
  DF:          {throughput_test.get('degrees_of_freedom', 0):>8d}

Note: For p-value calculation, use scipy.stats.ttest_rel in production

{'='*70}
AUTO-TUNE METRICS
{'='*70}

Convergence Iterations:
  Mean:   {statistics.mean(convergence_data) if convergence_data else 0:>8.1f}
  Median: {statistics.median(convergence_data) if convergence_data else 0:>8.1f}
  Min:    {min(convergence_data) if convergence_data else 0:>8d}
  Max:    {max(convergence_data) if convergence_data else 0:>8d}

Final Batch Bytes Selected (Auto-tune):
  Mean:   {statistics.mean(final_batch_bytes_data) if final_batch_bytes_data else 0:>8.0f} bytes ({avg_auto_batch_mb:.1f} MB)
  Median: {statistics.median(final_batch_bytes_data) if final_batch_bytes_data else 0:>8.0f} bytes ({statistics.median(final_batch_bytes_data) / 1048576 if final_batch_bytes_data else 0:.1f} MB)
  Min:    {min(final_batch_bytes_data) if final_batch_bytes_data else 0:>8d} bytes ({min(final_batch_bytes_data) / 1048576 if final_batch_bytes_data else 0:.1f} MB)
  Max:    {max(final_batch_bytes_data) if final_batch_bytes_data else 0:>8d} bytes ({max(final_batch_bytes_data) / 1048576 if final_batch_bytes_data else 0:.1f} MB)

Manual Batch Size: {manual_batch_mb:.1f} MB

Batch Size Comparison:
  Auto vs Manual: {batch_size_diff_pct:>+7.2f}%
  {"⚠️  Auto-tune selecting similar batch size to manual - may indicate manual config is already optimal" if abs(batch_size_diff_pct) < 10 else "✓ Auto-tune selecting different batch size"}

Skipped Batches:
  Mean:   {statistics.mean(skipped_data) if skipped_data else 0:>8.1f}
  Total:  {sum(skipped_data) if skipped_data else 0:>8d}

{'='*70}
STABILITY ASSESSMENT
{'='*70}
"""

    # Assess stability
    auto_cov = auto_throughput_stats.get('cov', 0)
    manual_cov = manual_throughput_stats.get('cov', 0)

    if auto_cov < 0.1 and manual_cov < 0.1:
        stability = "EXCELLENT - Both configurations show low variance"
    elif auto_cov < 0.2 and manual_cov < 0.2:
        stability = "GOOD - Acceptable variance for both configurations"
    elif auto_cov < 0.4 or manual_cov < 0.4:
        stability = "MODERATE - Some variance observed"
    else:
        stability = "POOR - High variance may affect results reliability"

    report += f"Network Stability: {stability}\n"
    report += f"Auto-tune CoV: {auto_cov:.2%}\n"
    report += f"Manual CoV: {manual_cov:.2%}\n\n"

    # Recommendations
    report += f"{'='*70}\n"
    report += "RECOMMENDATIONS\n"
    report += f"{'='*70}\n\n"

    if combined_improvement > 10:
        report += f"✓ Auto-tuning shows significant improvement ({combined_improvement:+.1f}%)\n"
    elif combined_improvement > 5:
        report += f"✓ Auto-tuning shows measurable improvement ({combined_improvement:+.1f}%)\n"
    elif combined_improvement > 0:
        report += f"~ Auto-tuning shows slight improvement ({combined_improvement:+.1f}%)\n"
    elif combined_improvement > -5:
        report += f"~ Performance is roughly equivalent ({combined_improvement:+.1f}%)\n"
    else:
        report += f"✗ Manual config performs better ({combined_improvement:+.1f}%)\n"

    if auto_cov > 0.4:
        report += "⚠ High variance in auto-tune results suggests unstable network\n"
        report += "  Consider running more samples or testing under stable conditions\n"

    cohen_d = throughput_test.get('cohen_d', 0)
    if abs(cohen_d) < 0.2:
        report += "⚠ Effect size is negligible - results may not be practically significant\n"
    elif abs(cohen_d) > 0.8:
        report += "✓ Large effect size - results are practically significant\n"

    # Check if manual config is already optimal
    if abs(batch_size_diff_pct) < 10 and abs(combined_improvement) < 5:
        report += "\n⚠️  MANUAL CONFIGURATION MAY BE ALREADY OPTIMAL\n"
        report += f"   Auto-tune is selecting batch sizes within 10% of manual ({batch_size_diff_pct:+.1f}%)\n"
        report += f"   and showing minimal performance difference ({combined_improvement:+.1f}%)\n"
        report += "\n   To validate auto-tuning effectiveness, try:\n"
        report += f"   1. Test with a suboptimal manual batch size (e.g., {int(manual_batch_mb / 4):.0f}MB or {int(manual_batch_mb * 2):.0f}MB)\n"
        report += "   2. Test under different network conditions (latency, bandwidth)\n"
        report += "   3. Test with different dataset sizes or table structures\n"

    report += f"\n{'='*70}\n"

    return report


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_results.py <benchmark_results_directory>")
        sys.exit(1)

    run_dir = Path(sys.argv[1])
    if not run_dir.exists():
        print(f"Error: Directory {run_dir} does not exist")
        sys.exit(1)

    print(f"Analyzing results from: {run_dir}")

    auto_results, manual_results = load_results(run_dir)

    if not auto_results or not manual_results:
        print("Error: No results found or incomplete data")
        sys.exit(1)

    print(
        f"Found {len(auto_results)} auto-tune runs and {len(manual_results)} manual runs")

    report = generate_report(run_dir, auto_results, manual_results)

    # Print to console
    print(report)

    # Save to file
    output_file = run_dir / "analysis_report.txt"
    with open(output_file, 'w') as f:
        f.write(report)

    print(f"\nReport saved to: {output_file}")


if __name__ == '__main__':
    main()
