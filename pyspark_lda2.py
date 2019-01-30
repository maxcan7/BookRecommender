# For pyspark context and table schemas
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
# For accessing system environment variables
import os, sys
# For processing tables
import re as re
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
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

    # Transform into vectors for word | topic distributions
    wordmat = model.topicsMatrix()
    word_top_table = sc.parallelize(wordmat)
    word_top_table = word_top_table.map(lambda x: [int(i) for i in x])
    word_top_table = sqlContext.createDataFrame(word_top_table)

    # Get model parameter information
    params = model.params
    params = sqlContext.createDataFrame(params)

    # Save text data to postgreSQL database on postgres_DB ec2 instance
    df_txts.write.format('jdbc') \
        .options(url='jdbc:postgresql://ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics',driver='org.postgresql.Driver',dbtable='text') \
        .mode('overwrite').save()

    # Save model parameters as separate table in same postgreSQL database
    params.write.format('jdbc') \
        .options(url='jdbc:postgresql://ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics',driver='org.postgresql.Driver',dbtable='params') \
        .mode('overwrite').save()

    # Save topic | document probability distributions as separate table in same postgreSQL database
    word_top_table.write.format('jdbc') \
        .options(url='jdbc:postgresql://ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics',driver='org.postgresql.Driver',dbtable='word_topic') \
        .mode('overwrite').save()

    # Save topic | document probability distributions as separate table in same postgreSQL database
    topic_doc_table.write.format('jdbc') \
        .options(url='jdbc:postgresql://ec2-54-205-173-0.compute-1.amazonaws.com/lda_booktopics',driver='org.postgresql.Driver',dbtable='topic_doc') \
        .mode('overwrite').save()

if __name__=="__main__":
    main(*sys.argv)
