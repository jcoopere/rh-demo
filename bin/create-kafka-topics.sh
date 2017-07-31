#!/bin/bash

# Determine location of this script.
dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Print usage.
printUsage() {
  echo "Usage: $0 <zookeeper quorum>"
  echo
  echo "e.g. $0 localhost:2181"
}

# Check args.
if [[ $# -ne 1 ]]; then
  printUsage
  exit 1
fi

zk=$1

# Create Kafka topics "cargo-in" and "cargo-out" in inbound and outbound data.
kafka-topics --zookeeper $zk --create --topic cargo-in --partitions 1 --replication-factor 1

kafka-topics --zookeeper $zk --create --topic cargo-out --partitions 1 --replication-factor 1
