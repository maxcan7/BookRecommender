# BookRecommender
My Insight Data Engineering Fellowship 2019A Project

This is still very much a WORK IN PROGRESS

There are some extraneous files and folders that will eventually be removed

Currently it is designed to run spark on a single node. From an EC2 instance, scrape project gutenberg, do some preprocessing (the clean.py script currently isn't working for me but everything else works), copy the data at multiple stages (raw, unzipped, and eventually cleaned) to s3, and then run an LDA topic model in pyspark or spark scala (whichever I can get working).

Eventually I want this to work distributed, with a nice front-end, where different versions of the model or different versions of the datasets are stored and can be retrieved efficiently (I will likely add wikipedia data later bc right now the gutenberg data is fairly small, only ~13GB)

The goal of this pipeline is to take book data, derive topics from that data using an LDA topic model, and use the probabilities from the topic model as the basis for a book recommendation engine akin to 8tracks for music
