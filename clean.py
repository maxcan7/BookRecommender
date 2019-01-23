# Copied from https://github.com/mbforbes/Gutenberg.git
# Cleans text from scrape_gutenberg.sh

'''
Looks for .txt files in <indir> and runs them through the strip_headers()
function, writing the result with the same filename to <outdir>.
'''

import glob
import sys

from tqdm import tqdm

import gutenberg.cleanup.strip_headers as strip_headers
from gutenberg._util.os import reopen_encoded
from gutenberg import Error


if len(sys.argv) != 3:
    print('usage: python3 clean.py <indir> <outdir>')
    sys.exit(1)

indir = sys.argv[1]
outdir = sys.argv[2]

files = glob.glob(indir + '/*.txt')
for f in tqdm(files):
    try:
        with reopen_encoded(open(f, 'r'), 'r', 'utf8') as infile:
            cleaned = strip_headers(infile.read())

        short = f.split('/')[-1]
        with open(outdir + '/' + short, 'w', encoding='utf8') as outfile:
            outfile.write(cleaned)
    except:
        print('Error processing', f, '; skipping...')
