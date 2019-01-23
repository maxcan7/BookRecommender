#!/usr/bin/env scala

# Script to set up credentials for s3 -> spark

# Specify credentials
sc.hadoopConfiguration.set("fs.s3a.access.key", "")
sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
