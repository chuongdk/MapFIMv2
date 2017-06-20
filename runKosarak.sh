echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/MApriori-0.0.1-SNAPSHOT.jar 
echo "Building .jar ............................................"
mvn package
echo "Running .................................................."



input="input/kosarakX1k.dat"
#zeroOne = 0.1%
zeroOne=990002
#zeroOne=990
memory=2000
maxDatabase=150


for i in 10 5 3 1
do 
	echo "I=$i"
	support=$((  $zeroOne * i  )) 
	echo "MapFIM   support = $support, memory = $memory,  input = $input,  max number of Database=$maxDatabase"
	hdfs dfs -rm -r /user/chuong/output

	echo "hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App $input output $support $memory $maxDatabase"

	hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App $input output $support $memory $maxDatabase
done




