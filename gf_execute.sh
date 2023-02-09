#!/bin/bash

if [ $# -eq 0 ]
then
  echo "No arguments supplied, expected Python script to execute."
  exit 1
fi

$SPARK_HOME/bin/spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 $1
