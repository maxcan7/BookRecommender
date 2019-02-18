#! /usr/bin/env python

# For pyspark context and schemas
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
# For accessing system environment variables
import os
import sys
# For processing dataframes
import re as re
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
import boto3
import fnmatch
import numpy
# For importing stopwords
import nltk
from nltk.corpus import stopwords
# For LDA
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.mllib.linalg import Vector
from pyspark.mllib.linalg import Vectors
from pyspark.ml.clustering import LDA


# Get aws key info from system
def aws_access(*argv):
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    return aws_access_key_id, aws_secret_access_key


# Set up spark context and s3 bucket and folder config
def s3_to_pyspark(config, aws_access_key_id, aws_secret_access_key):
    conf = SparkConf()
    conf.setMaster(config["publicDNS"])
    conf.setAppName("topicMakr")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    # Connect to bucket with boto3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config["bucket"])
    # Loop through all files and create a file list
    filelist = []
    for obj in bucket.objects.filter(Prefix=config["bucketfolder"]):
        if obj.size:
            filelist.append("s3n://" + bucket.name + "/" + obj.key)

    # Filter list to just books (named with numbers as per project gutenberg)
    filelist = fnmatch.filter(filelist, "s3n://" + bucket.name + "/" +
                              config["bucketfolder"] + "[0-9]*.txt")

    # Iterator function for processing partitioned data
    def preproc(iterator):
        strip = lambda document: document.strip()
        lower = lambda document: document.lower()
        split = lambda document: re.split(" ", document)
        alpha = lambda word: [x for x in word if x.isalpha()]
        minlen = lambda word: [x for x in word if len(x) > 3]
        nostops = lambda word: [x for x in word if x not in StopWords]
        for y in iterator:
            # Remove leading and trailing characters
            y = strip(y)
            # Make lowercase
            y = lower(y)
            # Tokenize words (split) by space
            y = split(y)
            # Remove words that have non-English alphabet characters
            y = alpha(y)
            # Remove words of size less than 3
            y = minlen(y)
            # Remove words from the nltk corpus stop words list
            y = nostops(y)
            yield y

    # Stopword list from nltk corpus
    StopWords = set(stopwords.words("english"))

    # Read text data from S3 Bucket
    tokens = []
    titles = []
    i = 0

    # Loop through all books in folder in bucket
    for file in filelist:
        # Load in all books and make each line a document
        books = sc.wholeTextFiles(file).map(lambda x: x[1])
        # Turn text to string for titles
        titles.append(books.collect()[0].encode('utf-8'))
        # Find 'Title:' in raw text
        start = titles[i].find('Title:')+7
        # Find the line break after the title
        end = titles[i][start:len(titles[i])].find('\r')
        end = start+end
        # Index title
        titles[i] = titles[i][start:end]
        # Run the preprocess function across books and zip
        tokens.append(books.mapPartitions(preproc).zipWithIndex())
        i += 1

    # Combine tokens
    tokens = sc.union(tokens)

    # Return variables for subsequent functions
    return sqlContext, tokens, titles


# Convert tokens to TF-IDF and run LDA model
def books_to_lda(ldaparam, sqlContext, tokens, titles):
    # Transform term tokens RDD to dataframe
    df_txts = sqlContext.createDataFrame(tokens, ["list_of_words", "index"])
    # Replace index to monotonically increasing set of values
    # (given zipWithIndex on tokens occurs over loop
    # and therefore is all zeros)
    df_txts = df_txts.withColumn('index', monotonically_increasing_id())
    # TF
    cv = CountVectorizer(inputCol="list_of_words",
                         outputCol="raw_features")
    cvmodel = cv.fit(df_txts)
    result_cv = cvmodel.transform(df_txts)

    # Create vocab list
    vocab = cvmodel.vocabulary

    # IDF
    idf = IDF(inputCol="raw_features",
              outputCol="features")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv)

    # Run LDA model
    lda = LDA(k=ldaparam["k"], maxIter=ldaparam["maxIter"])
    model = lda.fit(result_tfidf)
    return vocab, result_tfidf, model


# Set up tables and write to postgres
def postgres_tables(SQLconf, ldaparam, vocab,
                    result_tfidf, model, sqlContext, titles):

    # Get table for topic | document distributions
    top_doc_table = model.transform(result_tfidf)

    # Add titles to top_doc_table
    R = Row('index', 'title')
    df_titles = sqlContext \
        .createDataFrame([R(i, x) for i, x in enumerate(titles)])
    top_doc_table = top_doc_table.join(df_titles, on="index", how="outer") \
        .orderBy(F.col("index"))

    # Convert vectors to strings (for postgres)
    top_doc_table = top_doc_table.withColumn(
        "topicDistribution",
        top_doc_table["topicDistribution"]
        .cast(StringType())) \
        .withColumn("raw_features", top_doc_table["raw_features"]
                    .cast(StringType())) \
        .withColumn("features",
                    top_doc_table["features"]
                    .cast(StringType()))

    # Get top 7 words per topic
    topics = model.describeTopics(maxTermsPerTopic=SQLconf["topwords"])
    #
    # Add vocab to topics dataframe
    topics_rdd = topics.rdd
    topics_words = topics_rdd \
        .map(lambda row: row['termIndices']) \
        .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
        .collect()

    # Create top terms dataframe
    R = Row('topic', 'terms')
    df_topterms = sqlContext \
        .createDataFrame([R(i, x) for i, x in enumerate(topics_words)])
    topics = topics.join(df_topterms, on="topic", how="outer") \
        .orderBy(F.col("topic"))

    # Save dataframes to postgreSQL database on postgres_DB ec2 instance
    topics.write.format('jdbc') \
        .options(url=SQLconf["postgresURL"],
                 driver='org.postgresql.Driver', dbtable='topics') \
        .mode('overwrite').save()

    top_doc_table.write.format('jdbc') \
        .options(url=SQLconf["postgresURL"],
                 driver='org.postgresql.Driver', dbtable='documents') \
        .mode('overwrite').save()


# Set configurations
config = {
    "publicDNS": "spark://ec2-54-227-182-209.compute-1.amazonaws.com:7077",
    "bucket": "maxcantor-insight-deny2019a-bookbucket",
    "bucketfolder": "gutenberg_data/unzipped_data/"
}

ldaparam = {
    "k": 20,
    "maxIter": 100
}

SQLconf = {
    "topwords": 7,
    "postgresURL": "jdbc:postgresql://" +
    "ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics"
}

# Run pipeline functions
if __name__ == '__main__':
    [aws_access_key_id, aws_secret_access_key] = aws_access(*sys.argv)
    [sqlContext, tokens, titles] = s3_to_pyspark(
        config,
        aws_access_key_id,
        aws_secret_access_key)
    [vocab, result_tfidf, model] = books_to_lda(
        ldaparam, sqlContext,
        tokens, titles)
    postgres_tables(
        SQLconf, ldaparam, vocab, result_tfidf,
        model, sqlContext, titles)
