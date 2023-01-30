#!/bin/bash
pushd ~
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
start-all.sh
popd
