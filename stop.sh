#!/bin/bash
pushd ~
$SPARK_HOME/sbin/stop-all.sh
$HADOOP_HOME/sbin/stop-yarn.sh
$HADOOP_HOME/sbin/stop-dfs.sh
popd
