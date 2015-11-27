'''
Created on 22 Oct 2015

@author: xuepingpeng
'''
from mysql.connector import connection

from deepwalk.graph import Graph


class GraphContruction(object):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
     
def buildGraphAPA():
    G = Graph()
    cnx = connection.MySQLConnection(user='root', password='passwd',host='qcis4',database='dblp')
    cursor = cnx.cursor()
    query = ("SELECT name_hash, paper_hash FROM dblp.author")
    cursor.execute(query)
    for (name_hash, paper_hash) in cursor:
        G[name_hash].append(paper_hash)
        G[paper_hash].append(name_hash)
    cursor.close()
    cnx.close()
    G.make_consistent()
    
    return G   

def buildAuthorMap():
    
    authors = {}
    cnx = connection.MySQLConnection(user='root', password='passwd',host='qcis4',database='dblp')
    cursor = cnx.cursor()
    query = ("SELECT md5, name FROM dblp.author_md5")
    cursor.execute(query)
    for (md5, name) in cursor:
        authors[md5]=name
    cursor.close()
    cnx.close()

    return authors

def loadAuthorMap():
    
    authorFile = "../data/author-map.txt"
    authors = []
    with open(authorFile) as f:
        for l in f:
            authors.append(l)
    return authors
    