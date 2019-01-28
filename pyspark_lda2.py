from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import os, sys
import re as re
import nltk
from nltk.corpus import stopwords
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
    gutentext = sc.textFile("s3n://maxcantor-insight-deny2019a-bookbucket/gutenberg_data/unzipped_data/*.txt")

    # Preprocessing (remove stopwords, make lower case, etc.)
    StopWords = set(stopwords.words("english"))
    tokens = gutentext.map( lambda document: document.strip().lower()).map( lambda document: re.split(" ", document)).map( lambda word: [x for x in word if x.isalpha()]).map( lambda word: [x for x in word if len(x) > 3] ).map( lambda word: [x for x in word if x not in StopWords]).zipWithIndex()

    # Tf-IDF matrices for LDA model
    df_txts = sqlContext.createDataFrame(tokens, ["list_of_words",'index'])

    # TF
    cv = CountVectorizer(inputCol="list_of_words", outputCol="raw_features", vocabSize=5000, minDF=10.0)
    cvmodel = cv.fit(df_txts)
    result_cv = cvmodel.transform(df_txts)

    # IDF
    idf = IDF(inputCol="raw_features", outputCol="features")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv)

    # Run LDA model
    lda = LDA(k=20, maxIter=100)
    model = lda.fit(result_tfidf)

    # Visualizations for testing
    ll = model.logLikelihood(result_tfidf)
    lp = model.logPerplexity(result_tfidf)
    print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
    print("The upper bound on perplexity: " + str(lp))

    # Describe topics.
    topics = model.describeTopics(3)
    print("The topics described by their top-weighted terms:")
    topics.show(truncate=False)

    # Shows the result
    transformed = model.transform(result_tfidf)
    transformed.show(truncate=False)

if __name__=="__main__":
    main(*sys.argv)
