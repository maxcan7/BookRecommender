#!/bin/sh

filename="$SPARK_HOME/conf/spark-env.sh"

grep -F '$SPARK_HOME/conf/spark-env.sh' $filename
if [ $? -ne 0 ] ; then
echo '$SPARK_HOME/conf/spark-env.sh' >> $filename
fi

grep 'export JAVA_HOME=/usr' $filename
if [ $? -ne 0 ] ; then
echo 'export JAVA_HOME=/usr' >> $filename
fi

grep 'export SPARK_PUBLIC_DNS="ec2-3-89-11-202.compute-1.amazonaws.com"' $filename
if [ $? -ne 0 ] ; then
echo 'export SPARK_PUBLIC_DNS="ec2-3-89-11-202.compute-1.amazonaws.com"' >> $filename
fi
