if $(hdfs dfs -test -e text.txt)
then $(hdfs dfs -appendToFile file:///home/hadoop/local.txt text.txt)
else $(hdfs dfs -cp -f file:///home/hadoop/local.txt text.txt)
fi
