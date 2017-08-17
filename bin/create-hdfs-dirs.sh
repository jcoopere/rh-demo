#!/bin/bash

# Determine location of this script.
dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Print usage.
printUsage() {
  echo "Usage: $0"
  echo
  echo "e.g. $0"
}

# Check args.
if [[ $# -ne 0 ]]; then
  printUsage
  exit 0
fi


## Create HDFS dirs for training data and model output.
sudo -u hdfs hadoop fs -mkdir /iiot-demo
sudo -u hdfs hadoop fs -mkdir /iiot-demo/training-data
sudo -u hdfs hadoop fs -mkdir /iiot-demo/training-data/machine-state-classifier-data
sudo -u hdfs hadoop fs -mkdir /iiot-demo/training-data/ttf-regression-data
sudo -u hdfs hadoop fs -mkdir /iiot-demo/model

## Open permissions for all demo dirs.
sudo -u hdfs hadoop fs -chmod -R 777 /iiot-demo



