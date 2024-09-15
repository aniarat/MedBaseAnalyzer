import pandas as pd
import json

# Wczytaj plik CSV
file_path = 'modified_heart_disease_health_indicators.csv'
data = pd.read_csv(file_path)

max_records = 100000
data = data.head(max_records)  

# Przygotowanie zagnieżdżonej struktury JSON
patients = []
for i, row in data.iterrows():
    patient_document = {
        "patient_id": i + 1,
        "demographics": {
            "sex": int(row['Sex']),
            "age": row['Age'],
            "education": int(row['Education']),
            "income": int(row['Income'])
        },
        "health_status": {
            "gen_health": int(row['GenHlth']),
            "ment_health_days": int(row['MentHlth']),
            "phys_health_days": int(row['PhysHlth']),
            "difficulty_walking": bool(row['DiffWalk'])
        },
        "lifestyle": {
            "physical_activity": bool(row['PhysActivity']),
            "smoker": bool(row['Smoker']),
            "fruits": bool(row['Fruits']),
            "veggies": bool(row['Veggies'])
        },
        "diseases": {
            "heart_disease": bool(row['HeartDiseaseorAttack']),
            "stroke": bool(row['Stroke']),
            "diabetes": bool(row['Diabetes']),
            "high_blood_pressure": bool(row['HighBP']),
            "high_cholesterol": bool(row['HighChol'])
        }
    }
    patients.append(patient_document)

# Zapisz do pliku JSON
with open('patients_data.json', 'w') as f:
    json.dump(patients, f, indent=4)

print(f"Dane zostały przekształcone do formatu JSON. Liczba rekordów: {max_records}.")


# import pandas as pd

# # Wczytaj plik CSV
# file_path = 'modified_heart_disease_health_indicators.csv'
# data = pd.read_csv(file_path)

# # Przygotowanie zagnieżdżonej struktury JSON
# patients = []
# for i, row in data.iterrows():
#     patient_document = {
#         "patient_id": i + 1,
#         "demographics": {
#             "sex": int(row['Sex']),
#             "age": row['Age'],
#             "education": int(row['Education']),
#             "income": int(row['Income'])
#         },
#         "health_status": {
#             "gen_health": int(row['GenHlth']),
#             "ment_health_days": int(row['MentHlth']),
#             "phys_health_days": int(row['PhysHlth']),
#             "difficulty_walking": bool(row['DiffWalk'])
#         },
#         "lifestyle": {
#             "physical_activity": bool(row['PhysActivity']),
#             "smoker": bool(row['Smoker']),
#             "fruits": bool(row['Fruits']),
#             "veggies": bool(row['Veggies'])
#         },
#         "diseases": {
#             "heart_disease": bool(row['HeartDiseaseorAttack']),
#             "stroke": bool(row['Stroke']),
#             "diabetes": bool(row['Diabetes']),
#             "high_blood_pressure": bool(row['HighBP']),
#             "high_cholesterol": bool(row['HighChol'])
#         }
#     }
#     patients.append(patient_document)

# # Zapisz do pliku JSON
# import json
# with open('patients_data.json', 'w') as f:
#     json.dump(patients, f, indent=4)

# print("Dane zostały przekształcone do formatu JSON.")
