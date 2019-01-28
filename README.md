# BookRecommender
My Insight Data Engineering Fellowship 2019A Project

This is still very much a WORK IN PROGRESS

From an EC2 instance, scrape project gutenberg, do some preprocessing, copy the data at multiple stages (raw, unzipped, and eventually cleaned) to s3, and then run an LDA topic model in pyspark.

I will add wikipedia data later because right now the gutenberg data is fairly small, only ~13GB.

I also need to get the data and model outputs into Postgres, and eventually make some kind of front-end.

The goal of this pipeline is to create a framework for data scientists to access large quantities of book data, derive topics from the data, and store various iterations of data, model parameters, and model estimates (i.e. topic probabilities), with the hypothetical end-goal being a book recommendation system along the lines of 8tracks for music.
