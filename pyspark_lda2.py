# For pyspark context and schemas
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
# For accessing system environment variables
import os, sys
# For processing dataframes
import re as re
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StringType
import numpy
# For importing stopwords
import nltk
from nltk.corpus import stopwords
# For LDA
from pyspark.ml.feature import CountVectorizer , IDF
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.ml.clustering import LDA

def main(*argv):
    # Set up spark configuration and aws access
    conf = SparkConf()
    conf.setMaster("spark://ec2-54-227-182-209.compute-1.amazonaws.com:7077")
    conf.setAppName("pyspark_lda")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Read text data from S3 Bucket
    gutentext = sc.wholeTextFiles("s3n://maxcantor-insight-deny2019a-bookbucket/gutenberg_data/unzipped_data/*.txt") \
        .map(lambda x: x[1])

    # Extract titles from gutenberg text
    titles = [None] * len(gutentext.collect())
    for i in range(len(gutentext.collect())):
        # Convert from unicode to string
        titles[i] = gutentext.collect()[i].encode('utf-8')
        # Find index of 'T' in 'Title:' and add 7 to get to start of title
        start = titles[i].find('Title:')+7
        # Find break after title as endpoint
        end = titles[i][start:len(titles[i])].find('\r')
        end = start+end
        # Subset to just title
        titles[i] = titles[i][start:end]

    # Preprocessing (remove stopwords, make lower case, etc.)
    StopWords = set(stopwords.words("english"))
    tokens = gutentext.map( lambda document: document.strip().lower()) \
        .map( lambda document: re.split(" ", document)) \
        .map( lambda word: [x for x in word if x.isalpha()]) \
        .map( lambda word: [x for x in word if len(x) > 3] ) \
        .map( lambda word: [x for x in word if x not in StopWords]) \
        .zipWithIndex()

    # Tf-IDF matrices for LDA model
    df_txts = sqlContext.createDataFrame(tokens, ["list_of_words","index"])

    # TF
    cv = CountVectorizer(inputCol="list_of_words", \
        outputCol="raw_features")
    cvmodel = cv.fit(df_txts)
    result_cv = cvmodel.transform(df_txts)

    # Create vocab list
    vocab = cvmodel.vocabulary

    # IDF
    idf = IDF(inputCol="raw_features", outputCol="features")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv)

    # Run LDA model
    k = 20
    lda = LDA(k=k, maxIter=100)
    model = lda.fit(result_tfidf)

    # Get table for topic | document distributions
    top_doc_table = model.transform(result_tfidf)

    # Add titles to top_doc_table
    R = Row('index','title')
    df_titles = sqlContext.createDataFrame([R(i, x) for i, x in enumerate(titles)])
    top_doc_table = top_doc_table.join(df_titles, on = "index", how = "outer").orderBy(F.col("index"))

    # Convert raw_features, features, and topicDistributions to strings (for postgres)
    top_doc_table = top_doc_table.withColumn("topicDistribution", top_doc_table["topicDistribution"].cast(StringType())) \
        .withColumn("raw_features", top_doc_table["raw_features"].cast(StringType())) \
        .withColumn("features", top_doc_table["features"].cast(StringType()))

    # Get top 7 words per topic
    wordNumbers = 7
    topics = model.describeTopics(maxTermsPerTopic = wordNumbers)

    # Add vocab to topics dataframe
    topics_rdd = topics.rdd
    topics_words = topics_rdd\
           .map(lambda row: row['termIndices'])\
           .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
           .collect()

    # Create top terms dataframe
    R = Row('topic','terms')
    df_topterms = sqlContext.createDataFrame([R(i, x) for i, x in enumerate(topics_words)])
    topics = topics.join(df_topterms, on = "topic", how = "outer").orderBy(F.col("topic"))

    # Save dataframes to postgreSQL database on postgres_DB ec2 instance
    topics.write.format('jdbc') \
        .options(url='jdbc:postgresql://ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics',driver='org.postgresql.Driver',dbtable='topics') \
        .mode('overwrite').save()

    top_doc_table.write.format('jdbc') \
        .options(url='jdbc:postgresql://ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics',driver='org.postgresql.Driver',dbtable='documents') \
        .mode('overwrite').save()

if __name__=="__main__":
    main(*sys.argv)
