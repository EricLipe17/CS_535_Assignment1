#!/bin/bash
pushd ~
if [[ "$1" != "" ]]; then
    JAR="$1"
else
    JAR=.
fi

if [[ "$2" != "" ]]; then
    CLS="$2"
else
    CLS="org.ass1.Main"
fi

if [[ "$3" != "" ]]; then
    MODE="$3"
else
    MODE="client"
fi

$SPARK_HOME/bin/spark-submit --deploy-mode $MODE --supervise --class $CLS $JAR
popd
