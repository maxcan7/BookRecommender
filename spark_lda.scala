#!/usr/bin/env scala

# Preprocess data into spark strings
val sparkSession = SparkSession.builder()

.appName("LDA topic modeling")
.master("local[*]").getOrCreate()
val corpus: RDD[String] = sparkSession.read.json(readPath).rdd

.map(row => row.getAs("text").toString)

# Tokenize and remove words from stopwords setup
val stopWords: Set[String] = sparkSession.read.text("stopwords.txt")

.collect().map(row => row.getString(0)).toSet

val vocabArray: Array[String] = corpus.map(_.toLowerCase.split("\\s"))

.map(_.filter(_.length > 3)

.filter(_.forall(java.lang.Character.isLetter)))

.flatMap(_.map(_ -> 1L))
.reduceByKey(_ + _)
.filter(word => !stopWords.contains(word._1))

.map(_._1).collect()

# Convert to wordcount vector for LDA model
val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

// Convert documents into term count vectors
val documents: RDD[(Long, Vector)] =

tokenized.zipWithIndex.map { case (tokens, id) =>

val counts = new mutable.HashMap[Int, Double]()
tokens.foreach { term =>

if (vocab.contains(term)) {

val idx = vocab(term)
counts(idx) = counts.getOrElse(idx, 0.0) + 1.0

}

}
(id, Vectors.sparse(vocab.size, counts.toSeq))

}

# Run LDA model with 100 iterations for 20 topics
# can also set the parameters alpha, beta, and setOptimizer
# Alpha affects prior over documents | topics
# setAlpha or setdocConcentration
# Beta affects prior over topics | words
# setBeta or setTopicConcentration

val lda = new LDA().setK(20).setMaxIterations(100)
val ldaMode = lda.run(documents)

# Save model
ldaModel.save(sparkSession.sparkContext, "path")

# For reference: Load model (TAKE THIS OUT, MOVE TO GIT REPO README)
# val sameModel = DistributedLDAModel.load("path")
