import logging
import os
from airflow import models
from airflow import DAG

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from helpers.sql_queries import SqlQueries

from operators.stage_pg import StageToPostgresOperator
from operators.truth_pg import TruthToPostgresOperator
from operators.data_quality import DataQualityOperator



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 7, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email_on_retry': False,
    'scheduler_interval': '0 5 * * *'
}

# determine if need to load staging tables
loadStageTable = True

# determine if need to build and load truth tables
loadTableStates = True
loadTableCaseStatus = True
loadTableUSCityDemographics = True
loadTableWorldHappiness = True
loadTableH1BNationality = True
loadTableH1BPetitions = True
loadTableMinWage = True


def getPostgresConnId():
    return 'postgres_h1b_visa'


dag = DAG('H1B-Visa',
          default_args=default_args,
          description='Load and transform data in Postgres with Airflow',
          schedule_interval='@daily',
          catchup=False
          )

dag.doc_md = """
#### DAG Summary

This DAG loads files into staging tables, performs data extractions
and necessary transformations before loading the data into the
final *truth* tables used by analysts.  

#### Outputs

This pipeline produces the following staging tables:

- `stage_h1b_petitions`
- `stage_min_wage`
- `stage_nationality`
- `stage_us_city_demographics`
- `stage_world_happiness`

and final *truth* tables used by analysts:

- `case_status`
- `h1b_nationality`
- `h1b_petitions`
- `min_wage`
- `states`
- `us_city_demographics`
- `world_happiness`

#### Owner

For any questions or concerns, please contact 
[John Sinues](mailto:walkandride@hotmail.com).
"""


staging_dict = {'stage_nationality': './data/fys97-16_nivdetailtable.csv|Y',
                'stage_h1b_petitions': './data/h1b_kaggle.csv|Y',
                'stage_min_wage': './data/MinimumWageData.csv|Y',
                'stage_world_happiness': './data/Original_2017_full.csv|Y',
                'stage_us_city_demographics': './data/us-cities-demographics.csv|Y'}

truth_dict = {'stage_h1b_petitions': 'h1b_petitions',
              'stage_min_wage': 'min_wage',
              'stage_world_happiness': 'world_happiness',
              'stage_us_city_demographics': 'us_city_demographics'}

dq_checks = [
    {'dual_sql1': "SELECT COUNT(*) FROM stage_nationality WHERE year IS NOT NULL",
     'dual_sql2': "SELECT COUNT(*) FROM h1b_nationality",
     'descr': "# records in stage_nationality table (year not null) and # h1b_nationality records"}
]


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

db_check_task = PostgresOperator(
    task_id='db_check',
    dag=dag,
    postgres_conn_id=getPostgresConnId(),
    sql='''SELECT NOW();'''
)

create_staging_tables_task = PostgresOperator(
    task_id='create_staging_tables',
    dag=dag,
    postgres_conn_id=getPostgresConnId(),
    sql='create_staging_tables.sql',
)

stage_nationality_task = StageToPostgresOperator(
    task_id='stage_nationality',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='stage_nationality',
    filename='./data/fys97-16_nivdetailtable.csv',
    delimiter=';',
    loadTable=loadStageTable
)

stage_h1b_petitions_task = StageToPostgresOperator(
    task_id='stage_h1b_petitions',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='stage_h1b_petitions',
    filename='./data/h1b_kaggle.csv',
    delimiter=',',
    loadTable=loadStageTable
)

stage_min_wage_task = StageToPostgresOperator(
    task_id='stage_min_wage',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='stage_min_wage',
    filename='./data/MinimumWageData.csv',
    delimiter=',',
    loadTable=loadStageTable
)

stage_world_happiness_task = StageToPostgresOperator(
    task_id='stage_world_happiness',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='stage_world_happiness',
    filename='./data/Original_2017_full.csv',
    delimiter=',',
    loadTable=loadStageTable
)

stage_us_city_demographics_task = StageToPostgresOperator(
    task_id='stage_us_city_demographics',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='stage_us_city_demographics',
    filename='./data/us-cities-demographics.csv',
    delimiter=';',
    loadTable=loadStageTable
)

stage_data_quality_task = DataQualityOperator(
    task_id='stage_data_quality',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    check_data=staging_dict,
    type='stage'
)

truth_states_task = TruthToPostgresOperator(
    task_id='truth_states',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='states',
    sql=SqlQueries.build_states,
    loadTable=loadTableStates
)

truth_case_status_task = TruthToPostgresOperator(
    task_id='truth_case_status',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='case_status',
    sql=SqlQueries.build_case_status,
    loadTable=loadTableCaseStatus
)

truth_min_wage_task = TruthToPostgresOperator(
    task_id='truth_min_wage',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='min_wage',
    sql=SqlQueries.build_min_wage,
    loadTable=loadTableMinWage
)

truth_us_city_demographics_task = TruthToPostgresOperator(
    task_id='truth_us_city_demographics',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='us_city_demographics',
    sql=SqlQueries.build_truth_us_city_demographics,
    loadTable=loadTableUSCityDemographics
)

truth_world_happiness_task = TruthToPostgresOperator(
    task_id='truth_world_happiness',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='world_happiness',
    sql=SqlQueries.build_truth_world_happiness,
    loadTable=loadTableWorldHappiness
)

truth_h1b_nationality_task = TruthToPostgresOperator(
    task_id='truth_h1b_nationality',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='h1b_nationality',
    sql=SqlQueries.build_truth_h1b_nationality,
    loadTable=loadTableH1BNationality
)

truth_h1b_petitions_task = TruthToPostgresOperator(
    task_id='truth_h1b_petitions',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    table_name='h1b_petitions',
    sql=SqlQueries.build_truth_h1b_petitions,
    loadTable=loadTableH1BPetitions
)

truth_data_quality_task1 = DataQualityOperator(
    task_id='truth_data_quality1',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    check_data=truth_dict,
    type='truth'
)

truth_data_quality_task2 = DataQualityOperator(
    task_id='truth_data_quality2',
    dag=dag,
    provide_context=True,
    conn_id=getPostgresConnId(),
    check_data=dq_checks,
    type='truth2'
)

create_table_relations_task = PostgresOperator(
    task_id='create_table_relations',
    dag=dag,
    postgres_conn_id=getPostgresConnId(),
    sql='create_relations.sql'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> \
    db_check_task >> \
    create_staging_tables_task >> \
    [stage_nationality_task, stage_h1b_petitions_task, stage_min_wage_task,
     stage_world_happiness_task, stage_us_city_demographics_task] >> \
    stage_data_quality_task >> \
    truth_states_task >> \
    truth_case_status_task >> \
    [truth_min_wage_task, truth_us_city_demographics_task, truth_world_happiness_task,
     truth_h1b_nationality_task, truth_h1b_petitions_task] >> \
    truth_data_quality_task1 >> \
    truth_data_quality_task2 >> \
    create_table_relations_task >> \
    end_operator
