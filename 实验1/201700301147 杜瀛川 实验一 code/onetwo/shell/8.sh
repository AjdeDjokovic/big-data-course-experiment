hdfs dfs -get text.txt
cat ../local.txt >> text.txt
hdfs dfs -copyFromLocal -f text.txt text.txt
