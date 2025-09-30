'''
step 1: creat a table for storing the data
step 2: Extract API data - https://api.nasa.gov/planetary/apod?api_key=kNA8WLEl0Vqvg0xU8W35bdGg7q2Fs1frbxJEB7ky
step 3: transform the data
step 4: load the data into db table
step 5: verify if data has been inserted in the table
'''

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.utils.dates import days_ago
import json

#Define the DAG
with DAG(
    dag_id = 'nasa_apod_etl_pipeline',
    #start_date=days_ago(1),
    #schedule_interval = '@daily',
    catchup =False
) as dag:
    
    # step 1: creat a table for storing the data
    # @task
    # def create_table():
    #     #create a connection
    #     postgres_hook=PostgresHook(postgres_conn_id="postgres_connection")

    #     #write the query
    #     create_table_query = '''
    #     CREATE TABLE IF NOT EXISTS nasa_apod (
    #     id SERIAL PRIMARY_KEY,
    #     title VARCHAR(255)
    #     url TEXT,
    #     date DATE,
    #     media_type VARCHAR(50)
    #     );

    #     '''
    #     #run the query using the connection
    #     postgres_hook.run(create_table_query)
    
    # step 2: Extract API data 
    # https://api.nasa.gov/planetary/apod?api_key=kNA8WLEl0Vqvg0xU8W35bdGg7q2Fs1frbxJEB7ky
    extract_nasa_apod=HttpOperator(
        task_id = 'extract_nasa_apod',
        http_conn_id='nasa_api',
        endpoint = 'planatery/apod',
        method = 'GET',
        data = {"api_key:{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response:response.json(),
    )

    #step 3: transform the data
    @task
    def transform_apod_data(response):
        apod_data={
            'title':response.get('title', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    #step 4: load the data into db table
    @task
    def load_data_to_postgres(apod_data):
        #create a connection
        postgres_hook=PostgresHook(postgres_conn_id="postgres_connection")

        insert_query = '''
        INSERT INTO nasa_apod (title, url, date, media_type)
        VALUES (%s, %s, %s, %s);
        '''
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
    #step 5: verify if data has been inserted in the table
    #create a connection
    @task
    def verify_table():
        postgres_hook=PostgresHook(postgres_conn_id="postgres_connection")

        #write the query
        verify_table_query = '''
        SELECT * from nasa_apod;
        '''
        #run the query using the connection
        postgres_hook.run(verify_table_query)

    #dependencies
    #create_table() >> extract_nasa_apod
    api_response = extract_nasa_apod.output
    transform_data = transform_apod_data(api_response)
    load_data_to_postgres(transform_data) >> verify_table()