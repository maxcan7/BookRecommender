
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

# Clean text of legalese and other gutenberg junk using py script from same creator as link above

# python setup
sudo -H python3 -m pip install --upgrade pip
sudo -H python3 -m pip install virtualenv
virtualenv -p python3 venv
source venv/bin/activate
pip3 install six
pip3 install tqdm

# run. <indir> contains all of your downloaded .txt files. <outdir> is where the
# script dumps the (relatively) cleaned versions.
# NOTE: Make sure Gutenberg is cloned! https://github.com/mbforbes/Gutenberg.git
python3 /home/ubuntu/Gutenberg/clean.py /home/ubuntu/gutenberg_data/ /home/ubuntu/gutenberg_data/cleaned/

# Web-access to S3: http://maxcantor-insight-deny2019a-bookbucket.s3.amazonaws.com/
# Copy cleaned data to S3 Bucket
aws s3 cp /home/ubuntu/gutenberg_data/ s3://maxcantor-insight-deny2019a-bookbucket/gutenberg_data/cleaned_data/ --recursive
