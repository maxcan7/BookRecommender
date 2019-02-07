# TopicMakr
My Insight Data Engineering Fellowship 2019A Project

This is still very much a WORK IN PROGRESS

From an EC2 instance, scrape project gutenberg, do some preprocessing, copy the data at multiple stages to s3, and then run an LDA topic model in pyspark.

The goal of this pipeline is to create a framework for data scientists to access large quantities of book data, derive topics from the data, and store various iterations of data, model parameters, and model estimates (i.e. topic probabilities), with the hypothetical end-goal being a book recommendation system along the lines of 8tracks for music.

Current Goals:
1. Get the code for title extraction to work efficiently at scale
2. Work on front end (dash example set up, need it to draw from postgres)
3. Clean up code (functionalize / make "pythonic")
4. Round probability vectors before turning them into strings and pushing to postgres (for readability)
5. Get more data (wikipedia scrape or something)
