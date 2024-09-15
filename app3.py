import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import timeit
import pandas as pd
from pymongo import MongoClient
import sqlalchemy
from sqlalchemy import text
import json
import plotly.graph_objs as go
from plotly.subplots import make_subplots

class DatabaseTester:
    def __init__(self):
        self.setup_databases()

    def setup_databases(self):
        self.mongo_client = MongoClient("mongodb://localhost:27017/")
        self.mongo_db = self.mongo_client["heart_disease_db"]
        self.mongo_collection = self.mongo_db["patients"]

        self.pg_engine = sqlalchemy.create_engine("postgresql://postgres@localhost:5432/db_heart_disease")

    def load_data_from_csv(self, filepath):
        data = pd.read_csv(filepath)
        return data.to_dict('records')
    
    def load_data_from_json(self, filepath):
        with open(filepath) as json_file:
            data1 = json.load(json_file)
        return data1

    # MongoDB  

    def get_max_patient_id_mongo(self):
        last_record = self.mongo_collection.find_one(sort=[("patient_id", -1)])
        return last_record['patient_id'] if last_record else 0
    
    def insert_mongo_from_file(self, data1, num_records):
        max_patient_id = self.get_max_patient_id_mongo()

        for i, record in enumerate(data1[:num_records]):
            
            if 'sex' not in record:  
                continue  
            
            patient_document = {
                "patient_id": max_patient_id + i + 1,
                "demographics": {
                    "sex": int(record['sex']),
                    "age": record['age'],
                    "education": int(record['education']),
                    "income": int(record['income'])
                },
                "health_status": {
                    "gen_health": int(record['genhlth']),
                    "ment_health_days": int(record['menthlth']),
                    "phys_health_days": int(record['physhlth']),
                    "difficulty_walking": bool(record['diffwalk'])
                },
                "lifestyle": {
                    "physical_activity": bool(record['phys_activity']),
                    "smoker": bool(record['smoker']),
                    "fruits": bool(record['fruits']),
                    "veggies": bool(record['veggies'])
                },
                "diseases": {
                    "heart_disease": bool(record['heartdiseaseorattack']),
                    "stroke": bool(record['stroke']),
                    "diabetes": bool(record['diabetes']),
                    "high_blood_pressure": bool(record['highbp']),
                    "high_cholesterol": bool(record['highchol'])
                }
            }
            self.mongo_collection.insert_one(patient_document)

    def read_mongo(self, limit):
        result = list(self.mongo_collection.find().limit(limit))
        return result
    
    def update_mongo(self, num_records):
    # Pobierz losowe dokumenty do aktualizacji
        patient_ids = [doc['patient_id'] for doc in self.mongo_collection.aggregate([{"$sample": {"size": num_records}}])]
        
        for patient_id in patient_ids:
            # Zwiększ pole 'income' o 1 dla każdego pacjenta
            self.mongo_collection.update_one(
                {"patient_id": patient_id}, 
                {"$inc": {"demographics.income": 1}}
            )

    
    def delete_mongo(self, num_records):
        patient_ids = [doc['patient_id'] for doc in self.mongo_collection.aggregate([{"$sample": {"size": num_records}}])]

        self.mongo_collection.delete_many({"patient_id": {"$in": patient_ids}})

    # licz pacjentów z wysokim cholesterolem i nadciśnieniem, pogrupowane według wykształcenia i z obliczeniem średniego wieku i dochodu.
    def complex_query_mongo_1(self, num_records):
        pipeline = [
            {"$match": {
                "diseases.high_cholesterol": True,
                "diseases.high_blood_pressure": True
            }},
            {"$group": {
                "_id": "$demographics.education",
                "average_age": {"$avg": "$demographics.age"},
                "average_income": {"$avg": "$demographics.income"},
                "patient_count": {"$sum": 1}
            }},
            {"$sort": {"average_income": -1}},
            {"$limit": num_records}
        ]
        
        result = list(self.mongo_collection.aggregate(pipeline))
        return result



    def complex_query_mongo_2(self, num_records):
        # Obliczamy średni dochód pacjentów
        avg_income_pipeline = [
            {"$group": {
                "_id": None,
                "average_income": {"$avg": "$demographics.income"}
            }}
        ]
        avg_income_result = list(self.mongo_collection.aggregate(avg_income_pipeline))
        avg_income = avg_income_result[0]['average_income'] if avg_income_result else 0

        # Główne zapytanie
        pipeline = [
            {"$match": {
                "demographics.income": {"$gt": avg_income}
            }},
            {"$group": {
                "_id": "$demographics.education",
                "average_income": {"$avg": "$demographics.income"},
                "patient_count": {"$sum": 1}
            }},
            {"$match": {
                "patient_count": {"$gt": 5},
                "average_income": {"$gt": 50000}
            }},
            {"$sort": {"average_income": -1}},
            {"$limit": num_records}
        ]

        result = list(self.mongo_collection.aggregate(pipeline))
        return result



    # PostgreSQL
    
    def get_max_patient_id_pg(self):
        connection = self.pg_engine.connect()
        query = text("SELECT MAX(patient_id) FROM patients")  
        result = connection.execute(query).fetchone()
        connection.close()
        
        return result[0] if result[0] is not None else 0

    def insert_postgresql_from_file(self, data, num_records):
        max_patient_id = self.get_max_patient_id_pg()
        connection = self.pg_engine.connect()

        patients_query = text("""
            INSERT INTO patients (patient_id, sex, age, education, income)
            VALUES (:patient_id, :sex, :age, :education, :income)
        """)
        
        lifestyle_query = text("""
            INSERT INTO lifestyle (patient_id, smoker, physical_activity, fruits, veggies)
            VALUES (:patient_id, :smoker, :physical_activity, :fruits, :veggies)
        """)

        health_status_query = text("""
            INSERT INTO health_status (patient_id, gen_health, ment_health_days, phys_health_days, difficulty_walking)
            VALUES (:patient_id, :gen_health, :ment_health_days, :phys_health_days, :difficulty_walking)
        """)

        diseases_query = text("""
            INSERT INTO diseases (patient_id, heart_disease, stroke, diabetes, high_blood_pressure, high_cholesterol)
            VALUES (:patient_id, :heart_disease, :stroke, :diabetes, :high_blood_pressure, :high_cholesterol)
        """)

        for i, record in enumerate(data[:num_records]):

            patient_id = max_patient_id + i + 1

            connection.execute(patients_query, {
                "patient_id": patient_id,
                "sex": record['Sex'],
                "age": record['Age'],
                "education": record['Education'],
                "income": record['Income']
            })

            connection.execute(lifestyle_query, {
                "patient_id": patient_id,
                "smoker": bool(record['Smoker']),
                "physical_activity": bool(record['PhysActivity']),
                "fruits": bool(record['Fruits']),
                "veggies": bool(record['Veggies'])
            })

            connection.execute(health_status_query, {
                "patient_id": patient_id,
                "gen_health": record['GenHlth'],
                "ment_health_days": record['MentHlth'],
                "phys_health_days": record['PhysHlth'],
                "difficulty_walking": bool(record['DiffWalk'])
            })

            connection.execute(diseases_query, {
                "patient_id": patient_id,
                "heart_disease": bool(record['HeartDiseaseorAttack']),
                "stroke": bool(record['Stroke']),
                "diabetes": bool(record['Diabetes']),
                "high_blood_pressure": bool(record['HighBP']),
                "high_cholesterol": bool(record['HighChol'])
            })

        connection.close()

    def read_postgresql(self, limit):
        connection = self.pg_engine.connect()

        query = text("""
            SELECT p.patient_id, p.sex, p.age, p.education, p.income,
                   l.smoker, l.physical_activity, l.fruits, l.veggies,
                   h.gen_health, h.ment_health_days, h.phys_health_days, h.difficulty_walking,
                   d.heart_disease, d.stroke, d.diabetes, d.high_blood_pressure, d.high_cholesterol
            FROM patients p
            JOIN lifestyle l ON p.patient_id = l.patient_id
            JOIN health_status h ON p.patient_id = h.patient_id
            JOIN diseases d ON p.patient_id = d.patient_id
            LIMIT :limit
        """)

        result = connection.execute(query, {'limit': limit}).fetchall()
        connection.close()
        return result
    
    def update_postgresql(self, patient_ids):
        connection = self.pg_engine.connect()

        patients_query = text("""
            UPDATE patients SET income = income + 1
            WHERE patient_id = ANY(:patient_ids)
        """)

        connection.execute(patients_query, {'patient_ids': patient_ids})
        connection.close()

    def delete_postgresql(self, num_records):
        connection = self.pg_engine.connect()

        query = text("SELECT patient_id FROM patients ORDER BY RANDOM() LIMIT :limit")
        patient_ids = [row[0] for row in connection.execute(query, {'limit': num_records})]

        connection.execute(text("DELETE FROM lifestyle WHERE patient_id = ANY(:patient_ids)"), {'patient_ids': patient_ids})
        connection.execute(text("DELETE FROM health_status WHERE patient_id = ANY(:patient_ids)"), {'patient_ids': patient_ids})
        connection.execute(text("DELETE FROM diseases WHERE patient_id = ANY(:patient_ids)"), {'patient_ids': patient_ids})
        connection.execute(text("DELETE FROM patients WHERE patient_id = ANY(:patient_ids)"), {'patient_ids': patient_ids})

        connection.close()

    def complex_query_postgresql_1(self, num_records):
        connection = self.pg_engine.connect()

        query = text("""
            SELECT p.education, 
                COUNT(p.patient_id) AS patient_count, 
                AVG(p.age) AS average_age, 
                AVG(p.income) AS average_income
            FROM patients p
            JOIN diseases d ON p.patient_id = d.patient_id
            WHERE d.high_cholesterol = TRUE 
            AND d.high_blood_pressure = TRUE
            GROUP BY p.education
            ORDER BY average_income DESC
            LIMIT :num_records;
        """)

        result = connection.execute(query, {'num_records': num_records}).fetchall()
        connection.close()
        return result



    def complex_query_postgresql_2(self, num_records):
        connection = self.pg_engine.connect()

        query = text("""
            WITH avg_income AS (
                SELECT AVG(income) AS avg_income
                FROM patients
            ),
            filtered_patients AS (
                SELECT p.education, 
                    AVG(p.income) AS average_income, 
                    COUNT(p.patient_id) AS patient_count
                FROM patients p
                JOIN avg_income ai ON p.income > ai.avg_income
                GROUP BY p.education
            )
            SELECT education, average_income, patient_count
            FROM filtered_patients
            WHERE patient_count > 5 
            AND average_income > 50000
            ORDER BY average_income DESC
            LIMIT :num_records;
        """)

        result = connection.execute(query, {'num_records': num_records}).fetchall()
        connection.close()
        return result



    # TESTS
    def run_tests(self, num_iterations=1):
        data_sizes = [10, 100, 1000, 10000]
        results = []

        csv_file = 'modified_heart_disease_health_indicators.csv'
        json_file = 'patients_data.json'

        csv_data = self.load_data_from_csv(csv_file)
        json_data = self.load_data_from_json(json_file)

        for size in data_sizes:
            pg_times = {'insert': [], 'read': [], 'update': [], 'delete': [], 'complex_query_1': [], 'complex_query_2': []}
            mongo_times = {'insert': [], 'read': [], 'update': [], 'delete': [], 'complex_query_1': [], 'complex_query_2': []}

            for _ in range(num_iterations):
                #MongoDB
                start_time = timeit.default_timer()
                self.insert_mongo_from_file(json_data, size)
                mongo_times['insert'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.read_mongo(size)
                mongo_times['read'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.update_mongo(size)
                mongo_times['update'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.delete_mongo(size)
                mongo_times['delete'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.complex_query_mongo_1(size)
                pg_times['complex_query_1'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.complex_query_mongo_2(size)
                pg_times['complex_query_2'].append(timeit.default_timer() - start_time)

                #PostgreSQL
                start_time = timeit.default_timer()
                self.insert_postgresql_from_file(csv_data, size)
                pg_times['insert'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                rows = self.read_postgresql(size)
                pg_times['read'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.update_postgresql([row[0] for row in rows])
                pg_times['update'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.delete_postgresql(size)
                pg_times['delete'].append(timeit.default_timer() - start_time)

                start_time = timeit.default_timer()
                self.complex_query_postgresql_1(size)
                pg_times['complex_query_1'].append(timeit.default_timer() - start_time)  

                start_time = timeit.default_timer()
                self.complex_query_postgresql_2(size)
                pg_times['complex_query_2'].append(timeit.default_timer() - start_time)      

            # Zapisz wyniki dla obu baz danych
            results.append({
                "size": size,
                "mongo_insert_avg": sum(mongo_times['insert']) / num_iterations,
                "mongo_read_avg": sum(mongo_times['read']) / num_iterations,
                "mongo_update_avg": sum(mongo_times['update']) / num_iterations,
                "mongo_delete_avg": sum(mongo_times['delete']) / num_iterations,
                "mongo_complex_query_1_avg": sum(mongo_times['complex_query_1']) / num_iterations,
                "mongo_complex_query_2_avg": sum(mongo_times['complex_query_2']) / num_iterations,
                "pg_insert_avg": sum(pg_times['insert']) / num_iterations,
                "pg_read_avg": sum(pg_times['read']) / num_iterations,
                "pg_update_avg": sum(pg_times['update']) / num_iterations,
                "pg_delete_avg": sum(pg_times['delete']) / num_iterations,
                "pg_complex_query_1_avg": sum(pg_times['complex_query_1']) / num_iterations,
                "pg_complex_query_2_avg": sum(pg_times['complex_query_2']) / num_iterations,

            })

        # Zapis wyników do pliku CSV
        self.save_results(results)

        return results


    def save_results(self, results):
        df = pd.DataFrame(results)
         # Dodanie aktualnej daty do nazwy pliku
        date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f'crud_performance_results_{date_str}.csv'
        
        df.to_csv(filename, index=False)

tester = DatabaseTester()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2("CRUD Operation Performance Comparison in PostgreSQL and MongoDB", className="text-center"), className="mb-4 mt-4")
    ]),
    dbc.Row([
        dbc.Col([
            dbc.Button("Run tests", id="run-tests", color="primary", className="me-2"),
            html.Div(id="loading-output", style={"margin-top": "10px"})
        ], width=12, className="text-center mb-4")
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-insert",
                type="default",
                children=dcc.Graph(id='insert-performance-chart')
            )
        ], width=6),
        dbc.Col([
            dcc.Loading(
                id="loading-read",
                type="default",
                children=dcc.Graph(id='read-performance-chart')
            )
        ], width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-update",
                type="default",
                children=dcc.Graph(id='update-performance-chart')
            )
        ], width=6),
        dbc.Col([
            dcc.Loading(
                id="loading-delete",
                type="default",
                children=dcc.Graph(id='delete-performance-chart')
            )
        ], width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-complex-query-1",
                type="default",
                children=dcc.Graph(id='complex-query-1-chart')
            )
        ], width=6),
        dbc.Col([
            dcc.Loading(
                id="loading-complex-query-2",
                type="default",
                children=dcc.Graph(id='complex-query-2-chart')
            )
        ], width=6)
    ])
], fluid=True)




import datetime

@app.callback(
    Output('insert-performance-chart', 'figure'),
    [Input('run-tests', 'n_clicks')]
)
def update_insert_chart(n_clicks):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    results = tester.run_tests()
    df = pd.DataFrame(results)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['size'], y=df['pg_insert_avg'], mode='lines+markers', name='PostgreSQL Insert'))
    fig.add_trace(go.Scatter(x=df['size'], y=df['mongo_insert_avg'], mode='lines+markers', name='MongoDB Insert'))

    fig.update_layout(
        title="Insert Time",
        xaxis_title="Data Size",
        yaxis_title="Time (s)"
    )
    return fig

@app.callback(
    Output('read-performance-chart', 'figure'),
    [Input('run-tests', 'n_clicks')]
)
def update_read_chart(n_clicks):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    results = tester.run_tests()
    df = pd.DataFrame(results)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['size'], y=df['pg_read_avg'], mode='lines+markers', name='PostgreSQL Read'))
    fig.add_trace(go.Scatter(x=df['size'], y=df['mongo_read_avg'], mode='lines+markers', name='MongoDB Read'))

    fig.update_layout(
        title="Read Time",
        xaxis_title="Data Size",
        yaxis_title="Time (s)"
    )
    return fig

@app.callback(
    Output('update-performance-chart', 'figure'),
    [Input('run-tests', 'n_clicks')]
)
def update_update_chart(n_clicks):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    results = tester.run_tests()
    df = pd.DataFrame(results)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['size'], y=df['pg_update_avg'], mode='lines+markers', name='PostgreSQL Update'))
    fig.add_trace(go.Scatter(x=df['size'], y=df['mongo_update_avg'], mode='lines+markers', name='MongoDB Update'))

    fig.update_layout(
       title="Update Time",
        xaxis_title="Data Size",
        yaxis_title="Time (s)"
    )
    return fig

@app.callback(
    Output('delete-performance-chart', 'figure'),
    [Input('run-tests', 'n_clicks')]
)
def update_delete_chart(n_clicks):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    results = tester.run_tests()
    df = pd.DataFrame(results)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['size'], y=df['pg_delete_avg'], mode='lines+markers', name='PostgreSQL Delete'))
    fig.add_trace(go.Scatter(x=df['size'], y=df['mongo_delete_avg'], mode='lines+markers', name='MongoDB Delete'))

    fig.update_layout(
        title="Delete Time",
        xaxis_title="Data Size",
        yaxis_title="Time (s)"
    )
    return fig

@app.callback(
    Output('complex-query-1-chart', 'figure'),
    [Input('run-tests', 'n_clicks')]
)
def update_complex_query_1_chart(n_clicks):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    results = tester.run_tests()
    df = pd.DataFrame(results)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['size'], y=df['pg_complex_query_1_avg'], mode='lines+markers', name='PostgreSQL Complex Query 1'))
    fig.add_trace(go.Scatter(x=df['size'], y=df['mongo_complex_query_1_avg'], mode='lines+markers', name='MongoDB Complex Query 1'))

    fig.update_layout(
        title="Complex Query 1",
        xaxis_title="Data Size",
        yaxis_title="Time (s)"
    )
    return fig

@app.callback(
    Output('complex-query-2-chart', 'figure'),
    [Input('run-tests', 'n_clicks')]
)
def update_complex_query_2_chart(n_clicks):
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate

    results = tester.run_tests()
    df = pd.DataFrame(results)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['size'], y=df['pg_complex_query_2_avg'], mode='lines+markers', name='PostgreSQL Complex Query 2'))
    fig.add_trace(go.Scatter(x=df['size'], y=df['mongo_complex_query_2_avg'], mode='lines+markers', name='MongoDB Complex Query 2'))

    fig.update_layout(
        title="Complex Query 2",
        xaxis_title="Data Size",
        yaxis_title="Time (s)"
    )
    return fig




if __name__ == '__main__':
    app.run_server(debug=True)

