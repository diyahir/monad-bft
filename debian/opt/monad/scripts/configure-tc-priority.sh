#!/bin/bash

if [ "$#" -eq 1 ]; then
    INTERFACE="$1"
else
    INTERFACE=$(ip route | grep default | awk '{print $5}' | head -n1)
    if [ -z "$INTERFACE" ]; then
        echo "Error: Could not determine default route interface"
        echo "Usage: $0 [interface]"
        exit 1
    fi
fi

if ! ip link show "$INTERFACE" > /dev/null 2>&1; then
    echo "Error: Interface $INTERFACE does not exist"
    exit 1
fi

tc qdisc del dev "$INTERFACE" root 2>/dev/null

echo "Setting up priority qdisc on $INTERFACE..."

if ! tc qdisc add dev "$INTERFACE" root handle 1: prio bands 2; then
    echo "Error: Failed to add priority qdisc"
    exit 1
fi

if ! tc qdisc add dev "$INTERFACE" parent 1:1 fq_codel; then
    echo "Error: Failed to add fq_codel to band 1"
    exit 1
fi

if ! tc qdisc add dev "$INTERFACE" parent 1:2 fq_codel; then
    echo "Error: Failed to add fq_codel to band 2"
    exit 1
fi

echo "TC priority configuration complete on $INTERFACE"
echo "To verify configuration, run: tc -s qdisc show dev $INTERFACE"