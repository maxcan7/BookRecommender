#!/usr/bin/env bash

source ./configurations.sh

export publicDNS
export bucket
export bucketfolder

export k
export maxIter

export topwords
export postgresURL

spark-submit \
    --jars /home/ubuntu/postgresql-42.2.5.jar \
    --class ranking \
    --master $spark_master_ui \
    /home/ubuntu/topicMakr_pyspark.py
