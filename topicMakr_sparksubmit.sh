#!/usr/bin/env bash

spark-submit \
    --jars /home/ubuntu/postgresql-42.2.5.jar \
    --class ranking \
    --master spark://54.227.182.209:6066 \
    /home/ubuntu/topicMakr_pyspark.py
