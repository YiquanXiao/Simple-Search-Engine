import pyspark
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.rdd import RDD
import csv
import numpy as np
from index import Index
from dataPreprocessing import tokenizer
from collections import defaultdict
from heapq import nlargest

# indexer = Index()
# indexer.build_inverted_index()
# print("Corpus Size:", indexer.get_corpus_size())  # expect 2
# print("Avg doc length:", indexer.get_avg_doc_length())  # expect 7
# print("Term Freq of 'animals' in test2.txt:", indexer.get_term_freq("animals", 0))  # expect 0
# print("Term Freq of 'animals' in test1.txt:", indexer.get_term_freq("animals", 1))  # expect 2
# print("Doc length of text2.txt:", indexer.get_doc_length(0))  # expect 6
# print("Doc length of text1.txt:", indexer.get_doc_length(1))  # expect 8
# print("Doc Freq of 'dog':", indexer.get_doc_freq("dog"))  # expect 2
# print("Doc Freq of 'shut':", indexer.get_doc_freq("shut"))  # expect 1


# query = "What ? and what?"
# query_freqs = defaultdict(int)
# for word in query.split():
#     query_token = tokenizer(word)
#     query_freqs[query_token] += 1
# print(query_freqs)


# Initialize dictionary
test_dict = {'gfg': 1, 'is': 4, 'best': 6, 'for': 7, 'geeks': 3}
 
# Initialize N
N = 3
 
# Printing original dictionary
print("The original dictionary is : " + str(test_dict))
 
# N largest values in dictionary
# Using nlargest
res = nlargest(N, test_dict, key=test_dict.get)
print(res)






