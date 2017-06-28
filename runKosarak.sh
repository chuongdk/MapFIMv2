echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/MApriori-0.0.1-SNAPSHOT.jar 
echo "Building .jar ............................................"
mvn package
echo "Running .................................................."


input="input/kosarakX1k.dat"
#zeroOne = 0.1%
zeroOne=990002

memory=4000
maxDatabase=150
nReducers=30
trieSize=10000000

for i in 10 8 6
do
        echo "I=$i"
        support=$((  $zeroOne * i  ))
        echo "MapFIM   support = $support, memory = $memory,  input = $input,  max number of Database=$maxDatabase"
        hdfs dfs -rm -r /user/chuong/output

        echo "hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App $input output $support $memory $maxDatabase"

        hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App $input output $support $memory $maxDatabase $nReducers /home/prof/chuong/workspace/eclat/src/eclat $trieSize

done


