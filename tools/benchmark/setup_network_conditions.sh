#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# Network condition simulator for testing auto-tuning under different scenarios
# Requires root/sudo access on Linux

set -euo pipefail

INTERFACE=${INTERFACE:-"eth0"}
CONDITION=${1:-""}

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

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
    if [[ "$(uname)" == "Darwin" ]]; then
        log_warn "Network simulation with tc is not available on macOS"
        log_warn "Consider using Network Link Conditioner in Xcode or dummynet"
        log_warn "See: https://developer.apple.com/download/more/ (search for Additional Tools)"
        exit 1
    fi

    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi

    if ! command -v tc &> /dev/null; then
        log_error "tc (traffic control) command not found"
        log_error "Install with one of:"
        log_error "  Ubuntu/Debian: apt-get install iproute2"
        log_error "  RHEL/CentOS 7: yum install iproute"
        log_error "  RHEL/CentOS 8+/Fedora: dnf install iproute"
        log_error "  Amazon Linux: yum install iproute-tc"
        exit 1
    fi
}

clean_qdisc() {
    log_info "Cleaning existing qdiscs on $INTERFACE..."
    tc qdisc del dev "$INTERFACE" root 2>/dev/null || true
    log_info "Network conditions reset to normal"
}

setup_stable() {
    log_info "Setting up STABLE network conditions"
    tc qdisc add dev "$INTERFACE" root netem delay 10ms 2ms
    log_info "Applied: 10ms delay ±2ms jitter"
}

setup_fast_network() {
    log_info "Setting up FAST network conditions (simulating local/fast connection)"
    tc qdisc add dev "$INTERFACE" root netem delay 5ms 1ms
    log_info "Applied: 5ms delay ±1ms (simulates local datacenter)"
}

setup_medium_network() {
    log_info "Setting up MEDIUM network conditions (simulating moderate connection)"
    tc qdisc add dev "$INTERFACE" root netem delay 50ms 5ms
    log_info "Applied: 50ms delay ±5ms (simulates cross-region network)"
}

setup_slow_network() {
    log_info "Setting up SLOW network conditions (simulating slow connection)"
    tc qdisc add dev "$INTERFACE" root netem delay 200ms 10ms
    log_info "Applied: 200ms delay ±10ms (simulates slow/distant network)"
}

setup_very_slow_network() {
    log_info "Setting up VERY SLOW network conditions (simulating high-latency connection)"
    tc qdisc add dev "$INTERFACE" root netem delay 500ms 20ms
    log_info "Applied: 500ms delay ±20ms (simulates satellite/very distant network)"
}

setup_packet_loss() {
    log_info "Setting up PACKET LOSS conditions"
    tc qdisc add dev "$INTERFACE" root netem loss 1% delay 10ms
    log_info "Applied: 1% packet loss, 10ms delay"
}

setup_bandwidth_limit() {
    log_info "Setting up BANDWIDTH LIMIT conditions"
    tc qdisc add dev "$INTERFACE" root tbf rate 100mbit burst 32kbit latency 400ms
    log_info "Applied: 100 Mbps bandwidth limit"
}

setup_congestion() {
    log_info "Setting up CONGESTION conditions"
    tc qdisc add dev "$INTERFACE" root netem delay 50ms 30ms loss 0.5% duplicate 0.1%
    log_info "Applied: 50ms±30ms delay, 0.5% loss, 0.1% duplicates"
}

show_current() {
    log_info "Current qdisc configuration for $INTERFACE:"
    tc qdisc show dev "$INTERFACE" || log_warn "No qdisc configured"
}

usage() {
    cat <<EOF
Usage: sudo $0 <condition>

Available conditions:
  clean          - Remove all network conditions (reset to normal)
  fast           - Fast network (5ms ±1ms) - local datacenter
  medium         - Medium network (50ms ±5ms) - cross-region
  slow           - Slow network (200ms ±10ms) - distant/slow connection
  very-slow      - Very slow network (500ms ±20ms) - satellite/extreme latency
  stable         - Minimal jitter (10ms ±2ms)
  packet-loss    - 1% packet loss with 10ms delay
  bandwidth      - 100 Mbps bandwidth limit
  congestion     - Simulate congested network
  show           - Show current configuration

Environment variables:
  INTERFACE      - Network interface to apply conditions (default: eth0)

Examples:
  sudo INTERFACE=ens5 $0 medium
  sudo $0 clean
  sudo $0 show

Note: This script only works on Linux. For macOS, use Network Link Conditioner.
EOF
}

main() {
    if [[ -z "$CONDITION" ]]; then
        usage
        exit 1
    fi

    case "$CONDITION" in
        clean)
            check_requirements
            clean_qdisc
            ;;
        fast)
            check_requirements
            clean_qdisc
            setup_fast_network
            ;;
        medium)
            check_requirements
            clean_qdisc
            setup_medium_network
            ;;
        slow)
            check_requirements
            clean_qdisc
            setup_slow_network
            ;;
        very-slow)
            check_requirements
            clean_qdisc
            setup_very_slow_network
            ;;
        stable)
            check_requirements
            clean_qdisc
            setup_stable
            ;;
        packet-loss)
            check_requirements
            clean_qdisc
            setup_packet_loss
            ;;
        bandwidth)
            check_requirements
            clean_qdisc
            setup_bandwidth_limit
            ;;
        congestion)
            check_requirements
            clean_qdisc
            setup_congestion
            ;;
        show)
            show_current
            ;;
        *)
            log_error "Unknown condition: $CONDITION"
            usage
            exit 1
            ;;
    esac

    show_current
}

main
