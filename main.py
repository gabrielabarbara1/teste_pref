import requests
import pandas as pd
import sqlite3

# Configurações da API
API_TOKEN = "x"
CITIES = ["São Paulo", "Rio de Janeiro", "Salvador"]

# Função para criar a conexão com o banco de dados
def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

# Função para criar a tabela de qualidade do ar
def create_table(cursor):
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS air_quality (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL,
        aqi INTEGER,
        pm25 REAL,
        pm10 REAL,
        no2 REAL,
        o3 REAL,
        temperature REAL,
        humidity REAL
    );
    ''')

# Função para obter dados da API
def fetch_air_quality_data(city):
    url = f"https://api.waqi.info/feed/{city}/?token={API_TOKEN}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == 'ok':
            return {
                "city": data['data']['city']['name'],
                "aqi": data['data']['aqi'],
                "pm25": data['data']['iaqi'].get('pm25', {}).get('v'),
                "pm10": data['data']['iaqi'].get('pm10', {}).get('v'),
                "no2": data['data']['iaqi'].get('no2', {}).get('v'),
                "o3": data['data']['iaqi'].get('o3', {}).get('v'),
                "temperature": data['data']['iaqi'].get('t', {}).get('v'),
                "humidity": data['data']['iaqi'].get('h', {}).get('v')
            }
    return None

# Função para inserir dados na tabela
def insert_air_quality_data(cursor, air_quality_data):
    cursor.execute("""
    INSERT INTO air_quality (city, aqi, pm25, pm10, no2, o3, temperature, humidity) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        air_quality_data['city'], air_quality_data['aqi'], air_quality_data['pm25'],
        air_quality_data['pm10'], air_quality_data['no2'], air_quality_data['o3'],
        air_quality_data['temperature'], air_quality_data['humidity']
    ))

# Função principal
def main():
    # Conectar ao banco de dados
    conn = create_connection('air_quality.db')
    cursor = conn.cursor()

    # Criar tabela
    create_table(cursor)

    # Coletar e inserir dados para cada cidade
    for city in CITIES:
        air_quality_data = fetch_air_quality_data(city)
        if air_quality_data:
            insert_air_quality_data(cursor, air_quality_data)
        else:
            print(f"Erro ao obter dados para {city}")

    # Salvar alterações
    conn.commit()

    # Consultar e exibir resultados
    cursor.execute("SELECT city, aqi, pm25, pm10 FROM air_quality")

    #Consultas solicitadas no desafio:
    """-- Consulta 1: Média do AQI por cidade
    SELECT city, AVG(aqi) as average_aqi FROM air_quality GROUP BY city;

    -- Consulta 2: Cidades com AQI acima de 100
    SELECT city, aqi FROM air_quality WHERE aqi > 100;"""

    results = cursor.fetchall()
    print(results)

    # Fechar cursor e conexão
    cursor.close()
    conn.close()

# Executar a função principal
if __name__ == "__main__":
    main()