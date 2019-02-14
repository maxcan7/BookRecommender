# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='TopicMakr'),

    html.Div(children='''
        LDA topic statistics produced from project gutenberg books.
    '''),

], style={'columnCount': 2})

if __name__ == '__main__':
    # Access by PUBLICDNS:8081 in browser
    app.run_server(debug=False,host = '0.0.0.0',port=8081)

