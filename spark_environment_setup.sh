#!/bin/sh

filename="$SPARK_HOME/conf/spark-env.sh"

grep 'export JAVA_HOME=/usr' $filename
if [ $? -ne 0 ] ; then
echo 'export JAVA_HOME=/usr' >> $filename
fi

grep 'export SPARK_PUBLIC_DNS="ec2-34-192-125-109.compute-1.amazonaws.com"' $filename
if [ $? -ne 0 ] ; then
echo 'export SPARK_PUBLIC_DNS="ec2-34-192-125-109.compute-1.amazonaws.com"' >> $filename
fi
