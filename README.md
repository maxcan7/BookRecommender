# TopicMakr
My Insight Data Engineering Fellowship 2019A New York Project

TopicMakr is a proof of concept for a topic modeling platform for data scientists to train topics for industry use-cases such as recommendation systems.

The project uses whole open source books scraped from Project Gutenberg. These books are read into an s3 bucket for processing and model training.

The pipeline is a Latent Dirichlet Allocation (LDA) topic model. The book data are preprocessed, tokenized, and trained in pyspark. The model trains a pre-configurable number of topics from the book data, producing distributions for the probabilities of words given topics, and for topics given documents. Topics may be interpreted by visual inspection of the top N most probable words per topic (often 7), and the books may then be characterized by the most probable topics for each book.

After the model is trained in pyspark, these data are stored in a postgres database, where some of the outputs may be viewed on topicMakr.ml, producing with a dash web app.

![Image of Pipeline](https://github.com/maxcan7/TopicMakr/blob/master/images/pipeline.png)
