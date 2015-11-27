#! /usr/bin/env python
# -*- coding: utf-8 -*-
from sets import Set
from deepwalk.graph import Graph
from mysql.connector import connection
import logging
from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg.distributed import CoordinateMatrix
from pyspark.sql import SQLContext

logger = logging.getLogger(__name__)
LOGFORMAT = "%(asctime).19s %(levelname)s %(filename)s: %(lineno)s %(message)s"


def buildGraphAPA():
    G = Graph()
    cnx = connection.MySQLConnection(user='root', password='passwd',host='qcis4',database='dblp')
    cursor = cnx.cursor()
#     query = ("SELECT distinct name_hash, paper_hash FROM dblp.author")
    query = ("SELECT name_hash, paper_hash FROM dblp.author_sample")
    cursor.execute(query)
    for (name_hash, paper_hash) in cursor:
        G[name_hash].append(paper_hash)
        G[paper_hash].append(name_hash)
    cursor.close()
    cnx.close()
    G.make_consistent()
    
    return G

def metaPathAPA(graph):
    
    nodes = list(graph.nodes())
    pathNumber = dict()
    authors = []
    for a in nodes:
        if a.startswith('a_'):
            authors.append(a) 
    distinct_authors = Set(authors)
    paths = []
    for node in distinct_authors:
        if len(graph[node]) > 0:
            for paper in graph[node]:
                if len(graph[paper]) > 0:
                    for author in graph[paper]:
                        if node != author:
                            path = []
                            path.append(node)
                            path.append(paper)
                            path.append(author)
                            paths.append(path)
                            key = str(node)+str(author)
                            if pathNumber.has_key(key):
                                value = pathNumber.get(key)+1.0
                                pathNumber[key] = value
                            else:
                                pathNumber[key] = 1.0
    return [distinct_authors,paths,pathNumber]


def pathNumber(paths,node1,node2):
    number = 0
    for path in paths:
        if path[0] == node1 and path[2]==node2:
            number = number + 1
    return number  

def process(sparkContext,sqlContext):

    print("Building Graph...")
    G_apa = buildGraphAPA()
    
    print("Meta Path...")
    paths = metaPathAPA(G_apa)
     
    print("Training...")
    authorIndex = []
    authorDegree = []
   
    authors = paths[0]
    pathNumber = paths[2]
    pathNumberAarry = []
    for pn in pathNumber.keys():
        pathNumberAarry.append(str(pn)+":"+str(pathNumber.get(pn)))
    
    index = 0
    for author in authors:
        authorDegree.append(str(author)+":"+str(len(G_apa[author])))
        authorIndex.append(str(author)+":"+str(index))
        index = index+1.0

    
#     unique_authors = authors 
     
    authorsRDD = sparkContext.parallelize(authors)
    authorIndex = sparkContext.parallelize(authorIndex)
    pathNumber = sparkContext.parallelize(pathNumberAarry)
    authorDegree = sparkContext.parallelize(authorDegree)
     
    
    
    authors = authorsRDD.collect()

    ai = authorIndex.collect()
    authorIndex = dict()
    for a in ai:
        p = a.split(":")
        authorIndex[p[0]]=p[1]
#     print authorIndex
    
    ad = authorDegree.collect()
    authorDegree = dict()
    for a in ad:
        p = a.split(":")
        authorDegree[p[0]]=p[1]
#     print authorDegree
    
    pn = pathNumber.collect()
    pathNumber = dict()
    for a in pn:
        p = a.split(":")
        pathNumber[p[0]]=p[1]
#     print pathNumber
    
    
    def matEntry(author,authors):
        row = []
#         for author in authors: 
        for a in authors:
            if author == a:
                row.append((long(float(authorIndex[author])),long(float(authorIndex[a])),1.0))
            else:
                key = str(author)+str(a)
                if pathNumber.has_key(key):
                    row.append((long(float(authorIndex[author])),long(float(authorIndex[a])), 2.0*float(pathNumber.get(key))/(float(authorDegree[author])+float(authorDegree[a]))))
                else:
                    row.append((long(float(authorIndex[author])),long(float(authorIndex[a])), 0.0))
        
        return row
    
    def matEntryNoArgs():
        row = []
        for author in authors: 
            for a in authors:
                if author == a:
                    row.append((long(float(authorIndex[author])),long(float(authorIndex[a])),1.0))
                else:
                    key = str(author)+str(a)
                    if pathNumber.has_key(key):
                        row.append((long(float(authorIndex[author])),long(float(authorIndex[a])), 2.0*float(pathNumber.get(key))/(float(authorDegree[author])+float(authorDegree[a]))))
                    else:
                        row.append((long(float(authorIndex[author])),long(float(authorIndex[a])), 0.0))
        
        return row

#     print matEntry() 
    print "memememememememmmmmmemmmm"  
    
    me = authorsRDD.map(matEntry(author,authors)).collect()#.reduce(lambda x,y: x.append(y))
#     me =  matEntry()
#     me =  matEntryNoArgs()
    print "memememememememmmmmmemmmmOoooooooooooooooo"  
    
    
    
    entries = sc.parallelize(me)  
    print "ssssssssssssssss"  
#     # Create an CoordinateMatrix from an RDD of MatrixEntries.
    mat = CoordinateMatrix(entries)
#      
    print mat
#     mat.saveAsTextFile("/home/xuepeng/uts/metapath.txt")
     
    # Get its size.
    print mat.numRows()  # 3
    print mat.numCols()  # 2
#     
    
      

if __name__ == "__main__":
    conf = SparkConf().setAppName("MetaPath")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    sqlContext.read().jdbc(url="jdbc:mysql://qcis4:3306/dblp?user=root&password=passwd", table="author_sample", column = "name_hash, paper_hash")
    process(sc,sqlContext)