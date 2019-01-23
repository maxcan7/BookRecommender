#!/bin/bash

# Run this on each node to set up spark standalone

# Install java-development-kit and scala
sudo apt-get update
sudo apt-get install openjdk-7-jdk scala

# Install sbt
wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb -P ~/Downloads
sudo dpkg -i ~/Downloads/sbt-0.13.7.deb
sudo apt-get install sbt

# Install Spark
wget https://archive.apache.org/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz -P ~/Downloads
sudo tar zxvf ~/Downloads/spark-1.6.1-bin-hadoop2.6.tgz -C /usr/local
sudo mv /usr/local/spark-1.6.1-bin-hadoop2.6 /usr/local/spark
sudo chown -R ubuntu /usr/local/spark

cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh

# Add necessary code to spark environment
bash spark_environment_setup.sh
