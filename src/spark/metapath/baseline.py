#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg.distributed import CoordinateMatrix
from pyspark.sql import SQLContext

logger = logging.getLogger(__name__)
# LOGFORMAT = "%(asctime).19s %(levelname)s %(filename)s: %(lineno)s %(message)s"

def process(sparkContext,sqlContext):
    
    # Define database connection parameters
    MYSQL_USERNAME = 'root'
    MYSQL_PASSWORD = 'passwd'
    MYSQL_CONNECTION_URL = "jdbc:mysql://qcis4:3306/dblp?user=" + MYSQL_USERNAME+"&password="+MYSQL_PASSWORD 
    
    df = sqlContext.read.format('jdbc').options(url=MYSQL_CONNECTION_URL,driver='com.mysql.jdbc.Driver', dbtable="dblp.author_sample").load()
 
    rows = df.select("name_hash").distinct().map(lambda r: r.name_hash).collect()
    colums = df.select("paper_hash").distinct().map(lambda r: r.paper_hash).collect()

    rawData = df.map(lambda p: (long(rows.index(p.name_hash)),long(colums.index(p.paper_hash)),1.0)).cache()

#   Create an CoordinateMatrix from an RDD of MatrixEntries.
    mat = CoordinateMatrix(rawData)
    
    rowMat = mat.toRowMatrix()
    
    print mat.numRows()  # 3
    print rowMat.numCols()  
    
#     transpose = rowMat.rows().zipWithIndex().map(lambda rvect, i : rvect.zipWithIndex().map( lambda ax, j : (j,(i,ax))))
    for r in rowMat.rows().collect():
        print r

      

if __name__ == "__main__":
    conf = SparkConf().setAppName("MetaPath")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
#     sqlContext.read().load(path, format, schema)
    process(sc,sqlContext)