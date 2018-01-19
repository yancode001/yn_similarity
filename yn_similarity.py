# -*- coding: utf-8 -*-

import sys, os
import re
import time
import datetime

#some parkages (like cassandra, SQL etc ) are not needed for this simple program

#so I commented out like the following

#cassandra related packages
#from cassandra.cluster import Cluster
#from cassandra.auth import PlainTextAuthProvider

#SQL related packages
#from pyspark.sql import Row
#from pyspark.sql import SQLContext


from pyspark import SparkContext,SparkConf
from collections import Counter
import itertools
from operator import add
import pdb

#set SPARK_HOME variables in code, 
os.environ["SPARK_HOME"] = "/home/yan/TensorFlowOnSpark/spark-2.2.0-bin-hadoop2.7/"
#my virtual environment
os.environ["PYSPARK_PYTHON"]="/home/yan/anaconda2/envs/tensorflow-conda/bin/python"

#some constant variables

#IP of spark master, replace your spark master IP below
SparkMaster_IP = "spark://192.168.1.108:7077"

#set this App Name
TestSimilarity = "TestSimilarity"

#depending on what kinds of requirements, we can add or remove some special chars in Special_Chars
#when we process words, we remove these special chars like some punctuations in words...
#I don't remove all punctuation marks like singl quotation '
#since it's often used inside single word. 

#for product level, it's better to include regular express to remove all unnecessary chars
Special_Chars = '&.,()[]{}:;"'


def load_and_process_data( ):

    #set SparkConf
    conf = SparkConf().setAppName( TestSimilarity ).setMaster( SparkMaster_IP )

    #set SparkContext
    sc = SparkContext(conf=conf)

    #set data path and filenames
    #for this assignment, read all data files as .txt file from 'data' subdirectory
    #here for this simple porgram, I just put several .TXT files as tetsing, the total size of words
    #is small, so read/load them into memory.
    #In product level, we need to divide/group files, so each time,
    #read/load only part of data into spark cluster workers' memory.
    #in product level, it's better to use No-SQL ( like cassandra database or MongoDB or other database )
    #data base for processing data depending on how large the total data is
    fulllines = sc.textFile( "data/*.txt" )


    #uncommenting out the following line can be running code in debug mode ( like step by step ): 
    #pdb.set_trace()

    #filter/remove all empty line or None
    line = fulllines.filter( lambda line:  line is not None or line is not "" )

    #remove all unnecessary chars in Special_Chars
    line = line.map( lambda line: "".join( [i for i in line if i not in Special_Chars ] ) )

    #replace all multiple spaces with just one space
    line = line.map( lambda line: line.replace( "  ", " " ) )

    #split all lines into words
    word_lst = line.flatMap( lambda line: line.split( ' ' ) )

    #the following result will be in the form of ( word, count )
    word_count_lst = word_lst.map( lambda word: ( word, 1 ) ).reduceByKey( add )

    #the following result will be in the form of ( any_pair ) = ( word1, word2 )
    pair_lst = line.flatMap( lambda line: itertools.combinations( line.split( ' ' ), 2 ) )

    #the following result will be in the form of ( word1 of pair, pair )
    word1_pair_lst = pair_lst.map( lambda x: (x[0], x ) )
    
    #join with word_count_lst using word1 as key
    #the following result will be in the form of ( word1 of pair, ( pair, word1 count ) )
    word1_pair_word1count_lst = word1_pair_lst.join( word_count_lst )

    #the following result will be in the form of ( word2 of pair, ( word1 of pair, word1 count ) )
    word2_word1_word1count_lst = word1_pair_word1count_lst.map( lambda x: ( x[1][0][1], ( x[0], x[1][1] ) ) )
 
    #join with word_count_lst using word2 as key 
    #the following result will be in the form of ( word2, ( ( word1, word1count), word2count ) )
    pair_count_lst = word2_word1_word1count_lst.join( word_count_lst )

    #the following result will be in the form of ( ( word2, word1 ),  similarity ))
    #similarity = min( word1_count, word2_count )/max( word1_count, word2_count )
    #here use jaccard similarity as the similarity of any two words
    #there are other similarities, but here I use jaccard similarity   
    pair_jaccard_similarity_lst = pair_count_lst.map (lambda x: ( ( x[0], x[1][0][0] ), min( float(x[1][0][1]), float( x[1][1] ) )/ max( float(x[1][0][1]), float( x[1][1] ) ) ) )
    
    #remove duplicated items
    pair_jaccard_similarity_lst = pair_jaccard_similarity_lst.distinct()
    print "beginning of displaying the final results"
    output = pair_jaccard_similarity_lst.collect()

    #print all final whole results
    print output

    #print out each item of final whole results
    for item in output:
        print item

    print "end of displaying the final results"

    #stop spark process, release resource etc
    sc.stop()


if __name__ == "__main__":

    if len(sys.argv) != 1:
        #here masterIP is the IP of spark master in spark cluster
        #7077 is default spark master IP. you can change it based on your spark master's setting
        #for this simple program, I removed some running parameters like --executor-memory 20G --total-executor-cores 96 etc
        print "usage: $SPARK_HOME/bin/spark-submit --master spark://masterIP:7077 yn_similarity.py "
        sys.exit( -1 )
    else:
        #keep the starting time
        startTime = datetime.datetime.now().replace(microsecond=0)

        #load and process data for this assignment
        load_and_process_data()

        #keep the ending time
        endTime = datetime.datetime.now().replace(microsecond=0)

        print "all needed time:", ( endTime - startTime )
        print "finished the processing!"
