import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from pymongo import MongoClient
import csv
import timeit
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import base64

class DatabaseTester:
    def __init__(self):
        self.setup_databases()

    def setup_databases(self):
        # Setup MongoDB
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.mongo_db = self.mongo_client["testdb"]
        self.mongo_collection = self.mongo_db["testcollection"]

    def load_data_from_csv(self, filepath):
        try:
            self.data_df = pd.read_csv(filepath)
            self.data = self.data_df.to_dict('records')
            return True
        except Exception as e:
            print(f"Error loading CSV: {str(e)}")
            return False

    def insert_mongo(self, data):
        self.mongo_collection.insert_many(data)

    def read_mongo(self):
        return list(self.mongo_collection.find())

    def update_mongo(self, data):
        for d in data:
            self.mongo_collection.update_one({"_id": d["_id"]}, {"$set": {"BMI": d["BMI"] + 1}})

    def delete_mongo(self, data):
        for d in data:
            self.mongo_collection.delete_one({"_id": d["_id"]})

    def complex_query_mongo(self):
        pipeline = [
            {"$match": {"HeartDiseaseorAttack": 1}},
            {"$group": {"_id": "$Sex", "average_BMI": {"$avg": "$BMI"}}},
            {"$sort": {"average_BMI": -1}}
        ]
        return list(self.mongo_collection.aggregate(pipeline))

    def run_tests(self):
        data_small = self.data[:10]
        data_medium = self.data[:100]
        data_large = self.data[:10000]

        datasets = [("small", data_small), ("medium", data_medium), ("large", data_large)]

        results = []

        for size, data in datasets:
            # MongoDB tests
            start_time = timeit.default_timer()
            self.insert_mongo(data)
            insert_time_mongo = timeit.default_timer() - start_time

            start_time = timeit.default_timer()
            read_data = self.read_mongo()
            read_time_mongo = timeit.default_timer() - start_time

            start_time = timeit.default_timer()
            self.update_mongo(read_data)
            update_time_mongo = timeit.default_timer() - start_time

            start_time = timeit.default_timer()
            self.delete_mongo(read_data)
            delete_time_mongo = timeit.default_timer() - start_time

            start_time = timeit.default_timer()
            complex_query_result = self.complex_query_mongo()
            complex_query_time_mongo = timeit.default_timer() - start_time

            results.append({
                "size": size,
                "insert_time_mongo": insert_time_mongo,
                "read_time_mongo": read_time_mongo,
                "update_time_mongo": update_time_mongo,
                "delete_time_mongo": delete_time_mongo,
                "complex_query_time_mongo": complex_query_time_mongo,
                "complex_query_result_mongo": complex_query_result
            })

        return results

    def save_results(self, results):
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"results_{now}.csv"
        keys = results[0].keys()
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(results)
        return filename

# Inicjalizacja obiektu DatabaseTester
tester = DatabaseTester()

# Utworzenie aplikacji Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("Database Test Dashboard", className="text-center"), className="mb-4 mt-4")
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select Files')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False
            )
        ], width=12)
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Button("Run Tests", id="run-tests", color="primary", className="me-2"),
            html.Div(id="loading-output", style={"margin-top": "10px"})
        ], width=12, className="text-center mb-4")
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading",
                type="default",
                children=dcc.Graph(id='insert-time-chart')
            )
        ], width=6),
        dbc.Col([
            dcc.Loading(
                id="loading2",
                type="default",
                children=dcc.Graph(id='read-time-chart')
            )
        ], width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading3",
                type="default",
                children=dcc.Graph(id='update-time-chart')
            )
        ], width=6),
        dbc.Col([
            dcc.Loading(
                id="loading4",
                type="default",
                children=dcc.Graph(id='delete-time-chart')
            )
        ], width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading5",
                type="default",
                children=dcc.Graph(id='complex-query-time-chart')
            )
        ], width=12)
    ])
], fluid=True, style={"background-color": "#FFE4E1", "font-family": "Arial, sans-serif"})

@app.callback(
    [Output('insert-time-chart', 'figure'),
     Output('read-time-chart', 'figure'),
     Output('update-time-chart', 'figure'),
     Output('delete-time-chart', 'figure'),
     Output('complex-query-time-chart', 'figure'),
     Output('loading-output', 'children')],
    [Input('run-tests', 'n_clicks')],
    [State('upload-data', 'contents')]
)
def update_dashboard(n_clicks, contents):
    if n_clicks is None or contents is None:
        raise dash.exceptions.PreventUpdate

    # Decode the uploaded file content
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    filepath = 'uploaded_file.csv'
    with open(filepath, 'wb') as f:
        f.write(decoded)

    # Informing about the ongoing process
    loading_output = "Running tests, please wait..."

    # Load data and run tests
    tester.load_data_from_csv(filepath)
    results = tester.run_tests()
    filename = tester.save_results(results)

    # Load the results from the CSV file
    df = pd.read_csv(filename)

    # Create charts
    fig_insert = px.bar(df, x='size', y='insert_time_mongo', title="Insert Time by Data Size")
    fig_read = px.bar(df, x='size', y='read_time_mongo', title="Read Time by Data Size")
    fig_update = px.bar(df, x='size', y='update_time_mongo', title="Update Time by Data Size")
    fig_delete = px.bar(df, x='size', y='delete_time_mongo', title="Delete Time by Data Size")
    fig_complex = px.bar(df, x='size', y='complex_query_time_mongo', title="Complex Query Time by Data Size")

    # Return the figures and the loading text
    return fig_insert, fig_read, fig_update, fig_delete, fig_complex, loading_output

if __name__ == '__main__':
    app.run_server(debug=True)
