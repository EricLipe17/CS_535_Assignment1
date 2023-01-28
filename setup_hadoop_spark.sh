printf "Enter Master Namenode (example: nashville)\n"
read first_namenode

printf "Enter Second Namenode: (example: madison)\n"
read second_namenode

printf "Enter Workers (example: jackson lansing albany)\n"
read -a workers

printf "Enter Port Range (example: 31601 31650)\n"
read -a port_range

#first_namenode=pierre
#second_namenode=olympia
#workers=("montgomery" "montpelier" "oklahoma-city" "phoenix" "providence" "raleigh" "madison" "nashville")
#port_range=(31476 31500)

first_port=${port_range[0]}
echo ""
echo ""
echo ""
echo "Master Namnode: $first_namenode"
echo "Second Namenode: $second_namenode"
echo "Workers: ${workers[*]}"
echo "Port Range: ${port_range[0]}-${port_range[1]}"

wget http://www.cs.colostate.edu/~cs435/PAs/PA0/hadoopConfTemplate.tar.gz >/dev/null 2>/dev/null
tar -xvf hadoopConfTemplate.tar.gz -C ~/. >/dev/null 2>/dev/null
wget http://www.cs.colostate.edu/~cs435/PAs/PA3/sparkConfTemplate.tar.gz >/dev/null 2>/dev/null
tar -xvf sparkConfTemplate.tar.gz -C ~/. >/dev/null 2>/dev/null

mainnode_core_site_4=$first_namenode
port_core_site_4=$first_port

sed -i "4s/NAMENODE_HOST/$mainnode_core_site_4/" $HOME/hadoopConf/core-site.xml
sed -i "4s/PORT/$port_core_site_4/" $HOME/hadoopConf/core-site.xml

port_mapred_site_16=$(($first_port+1))
sed -i "16s/PORT/$port_mapred_site_16/" $HOME/hadoopConf/mapred-site.xml

mainnode_hdfs_site_15=$first_namenode
port_hdfs_site_15=$(($first_port+2))
secondnode_hdfs_site_21=$second_namenode
port_hdfs_site_21=$(($first_port+3))
port_hdfs_site_27=$(($first_port+4))
port_hdfs_site_35=$(($first_port+5))
port_hdfs_site_43=$(($first_port+6))
sed -i "15s/NAMENODE_HOST/$mainnode_hdfs_site_15/" $HOME/hadoopConf/hdfs-site.xml
sed -i "15s/PORT/$port_hdfs_site_15/" $HOME/hadoopConf/hdfs-site.xml
sed -i "21s/SECOND_NAMENODE_HOST/$secondnode_hdfs_site_21/" $HOME/hadoopConf/hdfs-site.xml
sed -i "21s/PORT/$port_hdfs_site_21/" $HOME/hadoopConf/hdfs-site.xml
sed -i "27s/PORT/$port_hdfs_site_27/" $HOME/hadoopConf/hdfs-site.xml
sed -i "35s/PORT/$port_hdfs_site_35/" $HOME/hadoopConf/hdfs-site.xml
sed -i "43s/PORT/$port_hdfs_site_43/" $HOME/hadoopConf/hdfs-site.xml

mainnode_yarn_site_4=$first_namenode
port_yarn_site_4=$(($first_port+7))
mainnode_yarn_site_12=$first_namenode
port_yarn_site_12=$(($first_port+8))
mainnode_yarn_site_26=$first_namenode
port_yarn_site_26=$(($first_port+9))
port_yarn_site_39=$(($first_port+10))
port_yarn_site_74=$(($first_port+11))
port_yarn_site_80=$(($first_port+12))
port_yarn_site_86=$(($first_port+13))
sed -i "4s/HOST/$mainnode_yarn_site_4/" $HOME/hadoopConf/yarn-site.xml
sed -i "4s/PORT/$port_yarn_site_4/" $HOME/hadoopConf/yarn-site.xml
sed -i "12s/HOST/$mainnode_yarn_site_12/" $HOME/hadoopConf/yarn-site.xml
sed -i "12s/PORT/$port_yarn_site_12/" $HOME/hadoopConf/yarn-site.xml
sed -i "26s/HOST/$mainnode_yarn_site_26/" $HOME/hadoopConf/yarn-site.xml
sed -i "26s/PORT/$port_yarn_site_26/" $HOME/hadoopConf/yarn-site.xml
sed -i "39s/PORT/$port_yarn_site_39/" $HOME/hadoopConf/yarn-site.xml
sed -i "74s/PORT/$port_yarn_site_74/" $HOME/hadoopConf/yarn-site.xml
sed -i "80s/PORT/$port_yarn_site_80/" $HOME/hadoopConf/yarn-site.xml
sed -i "86s/PORT/$port_yarn_site_86/" $HOME/hadoopConf/yarn-site.xml

secondnode_masters_1=$second_namenode
sed -i "1s/SECONDARY_NAMENODE_HOST/$secondnode_masters_1/" $HOME/hadoopConf/masters

for j in "${workers[@]}"
do
      echo $j 
done >$HOME/hadoopConf/workers

mainnode_spark_defaults_29=$first_namenode
port_spark_defaults_29=$(($first_port+14))
folder_spark_defaults_31=$HOME"/sparkConf/logs"
sed -i "29s/<SPARK_MASTER_IP>/$mainnode_spark_defaults_29/" $HOME/sparkConf/spark-defaults.conf
sed -i "29s/<SPARK_MASTER_PORT>/$port_spark_defaults_29/" $HOME/sparkConf/spark-defaults.conf
sed -i "31s|hdfs://<NAMENODE_HOSTNAME>:<NAMENODE_PORT>/spark_log   #NOTE: This is NOT a unique port. It is namenode and port used in Hadoop setup.|$folder_spark_defaults_31|" $HOME/sparkConf/spark-defaults.conf


mainnode_spark_env_81=$first_namenode
port_spark_env_82=$port_spark_defaults_29
port_spark_env_83=$(($first_port+15))
sed -i "81s/SPARK_MASTER_IP=/SPARK_MASTER_IP=$mainnode_spark_env_81/" $HOME/sparkConf/spark-env.sh
sed -i "82s/SPARK_MASTER_PORT=/SPARK_MASTER_PORT=$port_spark_env_82/" $HOME/sparkConf/spark-env.sh
sed -i "83s/SPARK_MASTER_WEBUI_PORT=/SPARK_MASTER_WEBUI_PORT=$port_spark_env_83/" $HOME/sparkConf/spark-env.sh


sed -i "$(( $(wc -l <$HOME/sparkConf/workers)-5+1 )),$ d" $HOME/sparkConf/workers
for j in "${workers[@]}"
do
      echo $j 
done >>$HOME/sparkConf/workers

yes | $HADOOP_HOME/bin/hdfs namenode -format >/dev/null 2>/dev/null

echo ""
echo 'Spark and Hadoop Configurations are now complete. Please check and make sure that the courses/cs535/pa1 module is loaded with the "module list" command.'
echo ""
echo 'Start/stop hdfs with the "$HADOOP_HOME/sbin/start-dfs.sh"/"$HADOOP_HOME/sbin/stop-dfs.sh" commands'
echo ""
echo 'Start/stop yarn with the "$HADOOP_HOME/sbin/start-yarn.sh"/"$HADOOP_HOME/sbin/stop-yarn.sh" commands'
echo ""
echo 'Start/stop spark with the "start-all.sh"/"stop-all.sh" commands.'
echo ""
