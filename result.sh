#! /bin/bash
for ((i=1; i<=50; i++));
do
	cat $1/$i/part* > merge$i.txt 
done
cat $1/fromFPF/* > mergeFPF.txt
cat merge*.txt > final$1.txt
rm merge*.txt
