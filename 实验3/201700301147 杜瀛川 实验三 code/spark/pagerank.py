
import pyspark
from operator import add

#input:from,PR,toPages
#output:toPage,PR/len(oPages)
def compute_contribs(parts):
    toPages = parts[1][1].split(",")
    toPR = parts[1][0] / len(toPages)
    
    for toPage in toPages:
        yield(toPage,toPR)
        
#input:from \t toPages
#output:from,toPages
def link_function(line):
    parts = line.split("\t")
    return parts[0],parts[1]

#input:from,toPages
#output:from,1.0
def ini_function(parts):
    return parts[0],1.0

#.10f
def my(parts):
    return parts[0],float('%.10f' % parts[1])
    
    
def compute_pagerank(sc, url_data_file, fileOutPath,iterations):
    #from,toRages
    links = sc.textFile(url_data_file).map(link_function).cache()
    
    #links.foreach(print)
    
    #from,1.0
    rankValue = links.map(ini_function)
    
    #rankValue.foreach(print)
    
    for i in range(iterations):
        
        #from,PR,toPages           toPage,toPR
        contribs = rankValue.join(links).flatMap(compute_contribs)
        
        #contribs.foreach(print)
        
        #from,PR
        rankValue = contribs.reduceByKey(add).mapValues(lambda rank : rank * 0.85 + 0.15)
        
        #rankValue.foreach(print)
        
    rankValue.sortBy(keyfunc=lambda x:x[1],ascending = False).map(my).coalesce(1).saveAsTextFile(fileOutPath)
        
        
if __name__ == "__main__":
    
    sc.stop()
    
    filePath = "hdfs://localhost:9000/pagerank/DataSet.txt"
    fileOutPath = "hdfs://localhost:9000/pagerank/output.txt"
    
    sc = pyspark.SparkContext( 'local', 'pagerank')
    
    compute_pagerank(sc,filePath,fileOutPath,10)
    