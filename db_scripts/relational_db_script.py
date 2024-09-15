import pandas as pd

# Wczytaj plik CSV
data = pd.read_csv('modified_heart_disease_health_indicators.csv')

# Zmniejszenie rozmiaru do 50,000 wierszy
data = data.head(50000)

# Tworzenie tabeli Pacjenci
patients_df = data[['Sex', 'Age', 'Education', 'Income']].copy()
patients_df = patients_df.reset_index().rename(columns={"index": "patient_id"})

# Tworzenie tabeli Stan Zdrowia
health_status_df = data[['GenHlth', 'MentHlth', 'PhysHlth', 'DiffWalk']].copy()
health_status_df = health_status_df.reset_index().rename(columns={"index": "patient_id"})

# Tworzenie tabeli Styl Życia
lifestyle_df = data[['PhysActivity', 'Smoker', 'Fruits', 'Veggies']].copy()
lifestyle_df = lifestyle_df.reset_index().rename(columns={"index": "patient_id"})

# Tworzenie tabeli Choroby
diseases_df = data[['HeartDiseaseorAttack', 'Stroke', 'Diabetes', 'HighBP', 'HighChol']].copy()
diseases_df = diseases_df.reset_index().rename(columns={"index": "patient_id"})

# Zapisz tabele do plików CSV
patients_df.to_csv('patients.csv', index=False)
health_status_df.to_csv('health_status.csv', index=False)
lifestyle_df.to_csv('lifestyle.csv', index=False)
diseases_df.to_csv('diseases.csv', index=False)

print("Dane zostały podzielone i zapisane w plikach CSV.")
