echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/MApriori-0.0.1-SNAPSHOT.jar 
echo "Building .jar ............................................"
mvn package
echo "Running .................................................."

echo "accidents"
hdfs dfs -rm -r /user/chuong/output
hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/accidents.dat output 34018 100000 1000
