#!/bin/bash

if [ $# -eq 0 ]
then
  echo "No arguments supplied, expected Python script to execute."
  exit 1
fi

$SPARK_HOME/bin/spark-submit $1
