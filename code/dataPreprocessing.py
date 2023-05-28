import os
import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
import string

def list_move_right(A,a):
    for i in range(a):
        A.insert(0,A.pop())
    return A
    
    
def tokenizer(text):
    """
    tokenize the text by removing all punctuations and making all letters lowercase
    Parameters
    ----------
    text: str
        the text that needs to be tokenized

    Returns
    -------
    output: str
        the token
    """
    return text.translate(str.maketrans('', '', string.punctuation)).lower()


def create_inverted_index(corpus_path, stopwords_path, output_folder_path):
    """
    create an inverted index
    Parameters
    ----------
    corpus_path: str
        the path of the corpus that contains all files in the search engine
    
    stopwords_path: str
        the path of the stopwords.txt
    
    output_folder_path: str
        the path of the folder to store inverted_index csv and other file (crc)

    Returns
    -------

    """
    spark = SparkSession.builder.appName("InvertedIndex").getOrCreate()
    
    # get all stopwords
    stopwords = spark.read.text(stopwords_path).cache()
    stopwords = stopwords.select(explode(split(stopwords.value, " ")).alias("token"))
    # stopwords.show()
    
    # dataframe for inverted index
    schema = StructType([StructField('token', StringType(), True)])
    inverted_index = spark.createDataFrame([], schema)
    # inverted_index.show()
    
    for file_name in os.listdir(corpus_path):
        file_path = os.path.join(corpus_path, file_name)
        log_data = spark.read.text(file_path).cache()

        # get all words
        words = log_data.select(explode(split(log_data.value, " ")).alias("word"))
        # words.show()

        # tokenization
        tokens = words.rdd.map(lambda x: tokenizer(x[0]))
        tokens = spark.createDataFrame(tokens, schema="string").toDF("token")
        # tokens.show()
        
        # get token frequency
        freq = tokens.groupBy("token").count()
        freq = freq.withColumnRenamed("count",file_name[:-4])
        # freq.show()
        
        # remove stopwords
        freq = freq.join(stopwords, on="token", how="leftanti")
        # freq.show()
        
        # merge inverted_index with freq to get a new inverted index that contains info from this file
        ## Add missing columns to inverted_index
        for column in [column for column in freq.columns if column not in inverted_index.columns]: 
            inverted_index = inverted_index.withColumn(column, lit(0))
        # inverted_index.show()
        
        ## Add missing columns to freq 
        for column in [column for column in inverted_index.columns if column not in freq.columns]: 
            freq = freq.withColumn(column, lit(0))
        # freq.show()
        
        ## merge two dataframe
        inverted_index = inverted_index.unionByName(freq)
        exprs = {x: "sum" for x in inverted_index.columns if x != inverted_index.columns[0]}
        inverted_index = inverted_index.groupBy("token").agg(exprs)
        columns = inverted_index.columns  # record column name
        for i in range(len(columns)):
            if "sum(" in columns[i]:
                columns[i] = columns[i][4:-1]
        inverted_index = inverted_index.toDF(*columns)  # update column name
    
    # Write CSV file with column header (column names)
    inverted_index.write\
        .mode('overwrite')\
        .option("header", True)\
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
        .csv(output_folder_path)
    
    # rename the generated csv file
    for file_name in os.listdir(output_folder_path):
        if file_name[-3:] != "csv":
            continue
        file_path = os.path.join(output_folder_path, file_name)
        new_file_path = os.path.join(output_folder_path, "inverted_index.csv")
        os.rename(file_path, new_file_path)
    
    # stop the spark session
    spark.stop()


if __name__ == '__main__':
    create_inverted_index(corpus_path="../corpus", 
                          stopwords_path="../stopwords/stopwords.txt", 
                          output_folder_path="../inverted_index")




