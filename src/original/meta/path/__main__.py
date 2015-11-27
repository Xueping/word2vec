#! /usr/bin/env python
# -*- coding: utf-8 -*-
from sets import Set
from deepwalk.graph import Graph
from mysql.connector import connection
import logging
import sys
import numpy

logger = logging.getLogger(__name__)
LOGFORMAT = "%(asctime).19s %(levelname)s %(filename)s: %(lineno)s %(message)s"


def buildGraphAPA():
    G = Graph()
    cnx = connection.MySQLConnection(user='root', password='passwd',host='qcis4',database='dblp')
    cursor = cnx.cursor()
#     query = ("SELECT distinct name_hash, paper_hash FROM dblp.author limit 10")
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
                                value = pathNumber.get(key)+1
                                pathNumber[key] = value
                            else:
                                pathNumber[key] = 1.0
    return [distinct_authors,paths,pathNumber]




def process():

    print("Building Graph...")
    G_apa = buildGraphAPA()
    
    print("Meta Path...")
    paths = metaPathAPA(G_apa)
     
    print("Training...")
    authorIndex = dict()
    authorDegree = dict()
   
    unique_authors = paths[0]
    pathNumber = paths[2]
    
    index = 0
    for author in unique_authors:
        authorDegree[author] = len(G_apa[author])
        authorIndex[author] = index
        index = index+1
      
    print unique_authors
    print pathNumber
    print authorDegree 
    print authorIndex
    similarityMatrix = numpy.zeros(shape=(len(unique_authors),len(unique_authors)))
    for author1 in unique_authors:
        for author2 in unique_authors:
            if author1 == author2:
                similarityMatrix[authorIndex[author1]][authorIndex[author1]] = 1
            else:
                key = str(author1)+str(author2)
                if pathNumber.has_key(key):
                    similarityMatrix[authorIndex[author1]][authorIndex[author2]] = 2.0000*pathNumber.get(key)/(authorDegree[author1]+authorDegree[author2])
                else:
                    similarityMatrix[authorIndex[author1]][authorIndex[author2]] = 0
    print similarityMatrix
    print("Saving...")
    numpy.savetxt('/home/xuepeng/git/deepmetapath/data/matrix.txt', similarityMatrix)
    
def main():
    process()   

if __name__ == "__main__":
    sys.exit(main())
#     pool = Pool(processes=10) 
#     pool.apply_async(process)
#     pool.close()
#     pool.join()
#     p = Process(target=process)
#     p.start()
#     p.join()
