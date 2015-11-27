'''
Created on 10 Nov 2015

@author: xuepeng
'''
import os
os.environ['SPARK_CLASSPATH'] = "/home/xuepeng/.m2/repository/mysql/mysql-connector-java/5.1.37/mysql-connector-java-5.1.37.jar"

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

def wordCount():
    logFile = "/home/xuepeng/uts/spark-1.5.1-bin-hadoop2.6/README.md"  # Should be some file on your system
    sc = SparkContext("local", "Simple App")
    logData = sc.textFile(logFile)

    numAs = logData.filter(lambda s: 'a' in s).count()
    numBs = logData.filter(lambda s: 'b' in s).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
    
def loadMySQL():
    sc = SparkContext("local", "Simple App")
    
    sqlContext = SQLContext(sc)
    
    query = "(SELECT name_hash as name, paper_hash as paper from dblp.author limit 1000) as author"
    jdbcDF = sqlContext.read().load(path="jdbc:mysql://qcis4:3306/dblp?user=root&password=passwd", format="jdbc",schema = query)
    
    jdbcDF.show()
    
def testSC():
    sc = SparkContext("local", "Simple App")
    
    textFile = sc.textFile("/home/xuepeng/uts/spark-1.5.1-bin-hadoop2.6/README.md")
    
    print textFile.count()

if __name__ == '__main__':
    testSC()
    