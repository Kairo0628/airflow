from airflow import DAG
from airflow.decorators import task

import psycopg2
import requests
from datetime import datetime

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    user = "kairo_o"  # 본인 ID 사용
    password = "....."  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()

@task
def extract(url):
  result = requests.get(url)
  print('extract complete')
  return result.json()

@task
def transform(json):
  records = []
  for i in json:
    country = i['name']['official']
    if "'" in country:
      country = country.replace("'", "''")
    population = i['population']
    area = i['area']

    records.append([country, population, area])
  print('transform complete')
  return records

@task
def load(schema, table, records):
  cur = get_Redshift_connection()
  try:
    cur.execute("BEGIN;")
    cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
      CREATE TABLE {schema}.{table} (
        country VARCHAR(128),
        population INT,
        area FLOAT
      );
    """)

    for i in records:
      country = i[0]
      population = i[1]
      area = i[2]
      cur.execute(f"INSERT INTO {schema}.{table} VALUES ('{country}', '{population}', '{area}');")
    cur.execute("COMMIT;")
    print('load complete')
  except (Exception, psycopg2.DatabaseError) as e:
    cur.execute("ROLLBACK;")
    print(e)

with DAG(
    dag_id = 'country_info_dag',
    start_date = datetime(2025, 11, 1),
    schedule = '30 6 * * 6',
) as dag:
  url = 'https://restcountries.com/v3.1/all?fields=name,population,area'
  schema = 'kairo_o'
  table = 'country_info'
  
  records = transform(extract(url))
  load(schema, table, records)