#!/usr/bin/env python

# Built from https://medium.com/@connectwithghosh/topic-modelling-with-latent-dirichlet-allocation-lda-in-pyspark-2cb3ebd5678e as a point of reference

# importing libraries
import pandas as pd
import pyspark
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
# text processing
from nltk.corpus import stopwords
import re as re
from pyspark.ml.feature import CountVectorizer , IDF
# building the model
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.mllib.clustering import LDA, LDAModel

# reading the data
data = sqlContext.read.format("csv") \
   .options(header='true', inferschema='true') \
   .load(os.path.realpath("Womens Clothing E-Commerce Reviews.csv"))

# some additional preprocessing not in the scrape script
reviews = data.map(lambda x : x['Review Text']).filter(lambda x: x is not None)
StopWords = stopwords.words("english")
tokens = reviews                                                   \
    .map( lambda document: document.strip().lower())               \
    .map( lambda document: re.split(" ", document))          \
    .map( lambda word: [x for x in word if x.isalpha()])           \
    .map( lambda word: [x for x in word if len(x) > 3] )           \
    .map( lambda word: [x for x in word if x not in StopWords])    \
    .zipWithIndex()

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
# NOTE: How do I change parameters?
num_topics = 20
max_iterations = 100
lda_model = LDA.train(result_tfidf[['index','features']].map(list), k=num_topics, maxIterations=max_iterations)

# Display top 5 words per topic (temporary visualization, make this a separate script)
wordNumbers = 5
topicIndices = sc.parallelize(lda_model.describeTopics\(maxTermsPerTopic = wordNumbers))
def topic_render(topic):
    terms = topic[0]
    result = []
    for i in range(wordNumbers):
        term = vocabArray[terms[i]]
        result.append(term)
    return result
topics_final = topicIndices.map(lambda topic: topic_render(topic)).collect()
for topic in range(len(topics_final)):
    print ("Topic" + str(topic) + ":")
    for term in topics_final[topic]:
        print (term)
    print ('\n')
