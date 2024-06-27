from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_functions import extract_data_from_wiki_page, load_into_Bq, transform_data


dag = DAG(
    dag_id= 'stads',
    default_args= {
        
        "start_date": datetime(2024, 6, 25),
    },
    schedule_interval= None,
    catchup= False
)


extract_data_from_wikipedia = PythonOperator(
    task_id= "extract_data_from_wikipedia",
    python_callable= extract_data_from_wiki_page,
    provide_context= True,
    op_kwargs= {"URL": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag= dag
)

transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    provide_context= True,
    python_callable= transform_data,
    op_kwargs= {"data_path": 'stads.csv'},
    dag=dag
)

load_into_Bq = PythonOperator(
    task_id='load_into_Bq',
    provide_context= True,
    python_callable= load_into_Bq,
    op_kwargs= {'project_id': 'world-stadiums-427116', 
               'dataset_id': 'world_stadiums', 
                'table_id': 'stadiums', 
                'data_path': 'output/transformed_data.csv'},
    dag=dag
)


extract_data_from_wikipedia >> transform_wikipedia_data >> load_into_Bq