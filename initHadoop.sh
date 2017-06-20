echo "removing data"
rm -fr /hadoop/chuong/tmp/*
rm -fr /hadoop/chuong/yarn/* 
for i in {0..14}
do
	echo "ssh to slave $i"
	ssh chuong-slave-$i 'rm -fr /hadoop/chuong/*'
done
hadoop namenode -format
start-dfs.sh
start-yarn.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/chuong
hdfs dfs -mkdir input
echo "Copying data retail to hdfs ..."
hdfs dfs -put ../data/retail.dat input
#hdfs dfs -put ../data/webdocs.dat input
