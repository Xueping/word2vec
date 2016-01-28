'''
Created on 12 Jan 2016

@author: xuepeng
'''


from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import logging
import random
import sys

from gensim.models import Word2Vec
from scipy import spatial
from boto.sdb.db.sequence import double


def process(args):

    sentences = []
    with open(args.input) as f:
        for line in f:
            sentences.append(line.split())
    
    print("Training...")
    model = Word2Vec(sentences, size=args.representation_size, window=args.window_size, min_count=0, workers=args.workers)
    model.save_word2vec_format(args.output)

def splitFile(modelFile, itemFile,catFile):
    items = []
    categories = []
    with open(modelFile) as f:
        for l in f:
            if l.startswith("i_"):
                items.append(l)
            elif l.startswith("c_"):
                categories.append(l)
    
    f_items = open(itemFile, "w")
    f_items.write("".join(map(lambda x: str(x), items)))
    f_items.close()
    
    f_categories = open(catFile, "w")
    f_categories.write("".join(map(lambda x: str(x), categories)))
    f_categories.close()

def main():
    parser = ArgumentParser("classification",
                          formatter_class=ArgumentDefaultsHelpFormatter,
                          conflict_handler='resolve')
    
    parser.add_argument("-l", "--log", dest="log", default="INFO",
                      help="log verbosity level")

    parser.add_argument('--input', nargs='?', required=True,
                      help='Input graph file')
    
    parser.add_argument('--output', required=True,
                        help='Output representation file')
    
    parser.add_argument('--representation-size', default=10, type=int,
                        help='Number of latent dimensions to learn for each node.')
    
    parser.add_argument('--seed', default=0, type=int,
                        help='Seed for random walk generator.')
    
    parser.add_argument('--window-size', default=1, type=int,
                        help='Window size of skipgram model.')
    
    parser.add_argument('--workers', default=1, type=int,
                        help='Number of parallel processes.')
    
    
    args = parser.parse_args()
    process(args)
    
    

if __name__ == '__main__':
#    sys.exit(main())
    query = "34 -0.025854 -0.022679 0.030671 0.039841 0.043556 -0.022356 0.047726 -0.015364 -0.036075 0.001533"
    items = []
    results = {}
    query_vector = map(lambda x: float(x), query.split()[1:])
    with open("../../data/class-out1.txt") as f:
        for l in f:
            list =  l.split()
            cat = list[0]
            vector = map(lambda x: float(x), list[1:])
            # calculate the similarity of two vectors
            results[cat] = 1 - spatial.distance.cosine(query_vector, vector)
    #Sort the results
    results = sorted(results.items(), key=lambda x: (-x[1], x[0]))
    #return top 5
    print results[:5]
    
    
    model = Word2Vec.load_word2vec_format("../../data/class-out.txt")
    for node in model.most_similar("34",topn=5):
        print(node)
    
#     f_items = open("../../data/class-out1.txt", "w")
#     f_items.write("".join(map(lambda x: str(x), items)))
#     f_items.close()
#     print items.index(0)
    
    
    