
#!/bin/bash

# This script was inspired by: https://gist.github.com/mbforbes/cee3fd5bb3a797b059524fe8c8ccdc2b

# Run this in an aws EC2 instance

# This script is designed to scrape Project Gutenberg for all english language books, clean the text, and copy the output to the S3 bucket

# Install necessary tools
sudo apt-get update
sudo apt-get install s3cmd

# Scrape all English language books
wget -m -H -nd "http://www.gutenberg.org/robot/harvest?filetypes[]=txt&langs[]=en"

# Copy raw data to s3 bucket
aws s3 cp /home/ubuntu/gutenberg_data/ s3://maxcantor-insight-deny2019a-bookbucket/gutenberg_data/raw_data/ --recursive

# Preprocessing

# Remove extraneous outputs from wget
rm robots.txt
rm harvest*

# Remove all duplicate encodings in ISO-<something> and UTF-8 (as per link).
ls | grep -e "-8.zip" | xargs rm
ls | grep -e "-0.zip" | xargs rm

# Remove other non-standard format files (may need to manually adjust later)
rm 89-AnnexI.zip
rm 89-Descriptions.zip
rm 89-Contents.zip
rm 3290-u.zip
rm 5192-tex.zip
rm 10681-index.zip
rm 10681-body.zip
rm 13526-page-inames.zip
rm 15824-h.zip
rm 18251-mac.zip

# Unzip files and remove zips
sudo apt install unzip
unzip "*.zip"
rm *.zip

# Move all unzipped [name].txt into ./[name] directory
mv */*.txt ./

# Remove empty and other 'junk' directories
ls | grep -v "\.txt" | xargs rm -rf

# Copy unzipped data to s3 bucket
aws s3 cp /home/ubuntu/gutenberg_data/ s3://maxcantor-insight-deny2019a-bookbucket/gutenberg_data/unzipped_data/ --recursive
