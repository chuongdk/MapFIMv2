echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/MApriori-0.0.1-SNAPSHOT.jar 
echo "Building .jar ............................................"
mvn package
echo "Running alpha 10 beta 10"
hdfs dfs -rm -r /user/chuong/output
hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 169208 1 10000
echo "DONE..................."

