if $(hdfs dfs -test -e file:///home/hadoop/text.txt);
then $(hdfs dfs -copyToLocal text.txt ../text2.txt); 
else $(hdfs dfs -copyToLocal text.txt ../text.txt); 
fi
