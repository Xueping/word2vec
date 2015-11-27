#! /usr/bin/env python
# -*- coding: utf-8 -*-

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
import sys
import random

from gensim.models import Word2Vec
from mysql.connector import connection

from deepwalk import graph
from deepwalk.graph import Graph


logger = logging.getLogger(__name__)
LOGFORMAT = "%(asctime).19s %(levelname)s %(filename)s: %(lineno)s %(message)s"


def buildGraphAPA():
    G = Graph()
    cnx = connection.MySQLConnection(user='root', password='passwd',host='qcis4',database='dblp')
    cursor = cnx.cursor()
    query = ("SELECT name_hash, paper_hash FROM dblp.pap")
    cursor.execute(query)
    for (name_hash, paper_hash) in cursor:
        G[name_hash].append(paper_hash)
        G[paper_hash].append(name_hash)
    cursor.close()
    cnx.close()
    G.make_consistent()
    
    return G

def buildGraphPCP():
    G = Graph()
    cnx = connection.MySQLConnection(user='root', password='passwd',host='qcis4',database='dblp')
    cursor = cnx.cursor()
    query = ("SELECT paper_hash, conference FROM dblp.pcp")
    cursor.execute(query)
    for (paper_hash, conference) in cursor:
        G[conference].append(paper_hash)
        G[paper_hash].append(conference)
    cursor.close()
    cnx.close()
    G.make_consistent()
    
    return G

def process(args):

    print("Building Graph...")
    G_apa = buildGraphPCP()
#     G_apa = buildGraphAPA()
    
    print("walk...")
    walks = graph.build_deepwalk_corpus(G_apa, num_paths=args.number_walks,
                                        path_length=args.walk_length, alpha=0, rand=random.Random(args.seed))
    print("Training...")
    model = Word2Vec(walks, size=args.representation_size, window=args.window_size, min_count=0, workers=args.workers)
 
    print("Saving...")
    model.save_word2vec_format(args.output)
    
    
def loadModel(args):
    
    print("load...")
    model = Word2Vec.load_word2vec_format("../data/authors.txt")
    print(model.most_similar("a2",topn=10))
   


def main():
    parser = ArgumentParser("deepwalk",
                          formatter_class=ArgumentDefaultsHelpFormatter,
                          conflict_handler='resolve')
    
    parser.add_argument("-l", "--log", dest="log", default="INFO",
                      help="log verbosity level")


    parser.add_argument('--output', required=True,
                        help='Output representation file')
    
    parser.add_argument('--representation-size', default=100, type=int,
                        help='Number of latent dimensions to learn for each node.')
    
    parser.add_argument('--seed', default=0, type=int,
                        help='Seed for random walk generator.')
    
    parser.add_argument('--window-size', default=1, type=int,
                        help='Window size of skipgram model.')
    
    parser.add_argument('--workers', default=1, type=int,
                        help='Number of parallel processes.')
    
    parser.add_argument('--walk-length', default=10, type=int,
                      help='Length of the random walk started at each node')
    
    parser.add_argument('--number-walks', default=10, type=int,
                      help='Number of random walks to start at each node')
    
    args = parser.parse_args()
    numeric_level = getattr(logging, args.log.upper(), None)
    logging.basicConfig(format=LOGFORMAT)
    logger.setLevel(numeric_level)
    
    process(args)
#     loadModel(args)

    

if __name__ == "__main__":
    sys.exit(main())
