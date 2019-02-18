# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
from pandas import DataFrame
import dash_table

# Connect to lda_booktopics db
conn = psycopg2.connect(dbname="lda_booktopics", user="postgres",
                        host="ec2-54-205-173-0.compute-1.amazonaws.com",
                        port="5432")

# Open cursor to perform database operations
cur = conn.cursor()

# External css
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# Designate app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Operate on tables
cur.execute('SELECT "topic","terms" FROM topics ORDER BY topic;')
topic_terms = DataFrame(cur.fetchall())

# Rename columns to topic and top 7 terms
topic_terms = topic_terms \
    .rename(index=str, columns={0: "topic", 1: "top 7 terms"})

# Convert term rows to strings
topic_terms["top 7 terms"] = topic_terms["top 7 terms"].astype('|S')
# Remove brackets
topic_terms["top 7 terms"] = topic_terms["top 7 terms"] \
    .map(lambda x: x.lstrip('[').rstrip(']'))

# Split terms into separate columns
splitterms = topic_terms["top 7 terms"].str.split(",", n=6, expand=True)

# making seperate first name column from new data frame
topic_terms["First term"] = splitterms[0]
topic_terms["Second term"] = splitterms[1]
topic_terms["Third term"] = splitterms[2]
topic_terms["Fourth term"] = splitterms[3]
topic_terms["Fifth term"] = splitterms[4]
topic_terms["Sixth term"] = splitterms[5]
topic_terms["Seventh term"] = splitterms[6]

# Dropping old "top 7 terms" column
topic_terms.drop(columns=["top 7 terms"], inplace=True)

# App layout
app.layout = dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in topic_terms.columns],
    data=topic_terms.to_dict("rows"),)

if __name__ == '__main__':
    # Access by PUBLICDNS:port in browser
    app.run_server(debug=False, host='0.0.0.0', port=8081)

