echo "removing data"
rm -fr /tmp/hadoop*
hadoop namenode -format
start-dfs.sh
start-yarn.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/chuong
hdfs dfs -mkdir input
echo "Copying data retail to hdfs ..."
hdfs dfs -put ../data/retail.dat input
#hdfs dfs -put ../data/webdocs.dat input
