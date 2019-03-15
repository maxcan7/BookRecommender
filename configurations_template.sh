# Set configurations

spark_master_ui = "spark://" + "spark master node public IP" + ":6066"

publicDNS = "spark master public DNS" + ":7077"
bucket = "s3 bucket with book files"
bucketfolder = "folder path in s3 bucket containing book files"

k = "integer number of topics for LDA to learn"
maxIter = "integer number of iterations of LDA training"

topwords = "integer number of top words per topic to store in postgres table"
postgresURL = "jdbc:postgresql://" + "EC2 instance with postgres database" + "/" + "database name"