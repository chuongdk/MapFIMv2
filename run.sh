echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/MApriori-0.0.1-SNAPSHOT.jar 
echo "Building .jar ............................................"
mvn package
echo "Running ................................................."
echo "input output support memory #databases"

echo "Beta 50M"
hdfs dfs -rm -r /user/chuong/output
hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/retail.dat output 10 1024 1000 2 /home/chuong/workspace/eclat/src/eclat
