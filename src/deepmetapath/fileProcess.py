import sys

from gensim.models import Word2Vec

import graphConstruction


def processFile(modelFile, authorFile, paperFile):
    papers = []
    authors = []
    with open(modelFile) as f:
        for l in f:
            if l.startswith("a_"):
                authors.append(l)
            elif l.startswith("p_"):
                papers.append(l)
    
    f_authors = open(authorFile, "w")
    authors.insert(0, str(len(authors))+" 64\n")
    f_authors.write("".join(map(lambda x: str(x), authors)))
    f_authors.close()
    
    f_papers = open(paperFile, "w")
    papers.insert(0, str(len(papers))+" 64\n")
    f_papers.write("".join(map(lambda x: str(x), papers)))
    f_papers.close()
    
def loadModel(modelFile):
    
    print("load...")
    model = Word2Vec.load_word2vec_format(modelFile)
    authorMap = graphConstruction.buildAuthorMap()
    print(authorMap["a_d35ca3e11192678861ea643872790607"] )
    for node in model.most_similar("a_d35ca3e11192678861ea643872790607",topn=10):
        print(authorMap[node[0]] )
    print("-----------------------" )    
    print(authorMap["a_f65b4298ba4ca5f6b177725bfc6f54d6"] )
    for node in model.most_similar("a_f65b4298ba4ca5f6b177725bfc6f54d6",topn=10):
        print(authorMap[node[0]])
    print(model.most_similar("a_d35ca3e11192678861ea643872790607",topn=10))
    print(model.most_similar("a_f65b4298ba4ca5f6b177725bfc6f54d6",topn=10))
    

def main():
    
#     modelFile = "../data/authors-deepwalk.txt"
    modelFile = "../data/authors-metapath.txt"
    loadModel(modelFile)
    
#     tst = [(u'a_0b53427f03e95d29cb520c73a461cd6b', 0.7998098134994507),
#             (u'a_f03f20554437a1770da5d6c5c96d6ad7', 0.7652999758720398), 
#             (u'a_ed5ae5da70c8b486c4dd51880262fcaf', 0.751783013343811),
#              (u'a_f85a9a4604556a0965c89662acb9b4bc', 0.7466002702713013), 
#              (u'a_82c7ba69e643a5c9c4ace6b4378740e7', 0.7405052185058594), 
#              (u'a_13cb40fb13e135e0489eadb2d4470418', 0.7386195659637451), 
#              (u'a_698c8899f840ffea27b72d8b8fa1c751', 0.7368607521057129), 
#              (u'a_e062f54baa9e144b63dea14f876df9cb', 0.7207427024841309), 
#              (u'a_48d61a8ea7b109858985973e850b4421', 0.6574369072914124), 
#              (u'a_7996cd9ef08088448c71105cad872861', 0.6235119104385376)]
#     for l in tst:
#         print(l[0])
    
#    deep metapth
#     modelFile = "../data/metaPathModel.txt"
#     authorFile = "../data/authors-metapath.txt"
#     paperFile = "../data/papers-metapath.txt"
    #deepwalk
#     modelFile = "/Users/xuepingpeng/Documents/QCIS/RnD/data/deepwalk.txt"
#     authorFile = "../data/authors-deepwalk.txt"
#     paperFile = "../data/papers-deepwalk.txt"
#     processFile(modelFile,authorFile,paperFile)
    

if __name__ == "__main__":
    sys.exit(main())
