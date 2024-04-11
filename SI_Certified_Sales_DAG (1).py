import json
import sys
import logging
import time

# Uses the namespace prefix for all dag imports
NAMESPACE = 'localdevnamespace'
sys.path.append(f'/usr/local/airflow/dags/{NAMESPACE}/')
airflow_env='<<<ENV_SUFFIX>>>'
environment = airflow_env.upper()
source_db='ODS_PROD'


from airflow.models import Connection, DAG, Variable
from datetime import datetime, timedelta
#from airflow.utils.timezone import timezone
from airflow.operators.dummy    import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from ds_airflow.operators.ds_snowflake_datamover import SnowflakeDataMoverOperator
from airflow.sensors.external_task import ExternalTaskSensor
from ds_airflow.helpers.slack_helper import get_slack_task_failure_callback, get_slack_dag_failure_callback



logger = logging.getLogger("airflow.task")
target_database='CERTIFIED_'+environment
tags = [target_database, 'Sales Insights', 'Certified Sales']

region= {'DEV':'DEV', 'UAT':'UAT', 'PROD':'PRD'}
var1 = airflow_env.upper()
airflow_env1=(region[var1])

slack_channel={'DEV_slack_channel' : '#test-airflow-slack','UAT_slack_channel' : '#test-airflow-slack','PROD_slack_channel' : '#dts-occ-gda-airflow-alerts'}

var = airflow_env.upper()+'_slack_channel'
channel=(slack_channel[var])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 3),

    'email_on_failure': False, 
      
}

     
with DAG(dag_id=f'{NAMESPACE}_MODEL_CERTIFIED_SALES_DAG',
        default_args=default_args,  
        schedule_interval = None, 
        #timezone=timezone("US/Pacific"),  # Set the timezone to PST
        catchup = False,
        tags = tags,
        description='DAG for Trigerring Snowflake task for MODEL_CERTIFIED_SALES  schema tables',
	
        on_failure_callback=get_slack_dag_failure_callback(channel)
          
        ) as dag:

    start_task = DummyOperator(task_id='start', dag=dag)

    # Create an instance of SnowflakeDataMoverOperator for etl_execution_insert
    etl_execution_insert_task = SnowflakeDataMoverOperator(
        task_id = "etl_execution_insert_task",
        environment = environment,
        scenario ='ETL_EXECUTION_INSERT',
        payload = { 'package_name': 'MODEL_CERTIFIED_SALES', 'execution_status_id': '1' },
        dag = dag
    )

    procedure_name= "CALL SP_LOAD_DIM_SALES_USER('" + source_db + "','" + airflow_env1 + "'," + '{{ ti.xcom_pull(task_ids="etl_execution_insert_task") }}' + ")"

    logger.info(procedure_name)
    

    call_dim_sales_user = SnowflakeOperator(
        task_id="call_dim_sales_user",
        snowflake_conn_id='snowflake_tasks_conn',
        database = target_database,
        schema='SALES',
        sql=procedure_name,
        dag=dag,
        on_failure_callback=get_slack_task_failure_callback(channel))


    LOAD_DIM_ACCOUNT= "CALL SP_LOAD_DIM_ACCOUNT(" +"'" + source_db +"'"+ ","+"'" + airflow_env1 +"'"+ "," + '{{ ti.xcom_pull(task_ids="etl_execution_insert_task") }}' + ")"
    logger.info(LOAD_DIM_ACCOUNT)

    call_dim_account = SnowflakeOperator(
        task_id="call_dim_account",
        snowflake_conn_id='snowflake_tasks_conn',
        database = target_database,
        schema='COMMON',
        sql=LOAD_DIM_ACCOUNT,
        dag=dag,
        on_failure_callback=get_slack_task_failure_callback(channel))

    

    LOAD_DIM_SALES_ACCOUNT= "CALL SP_LOAD_DIM_SALES_ACCOUNT(" +"'" + source_db +"'"+ ","+"'" + airflow_env1 +"'"+ "," + '{{ ti.xcom_pull(task_ids="etl_execution_insert_task") }}' + ")"

    logger.info(LOAD_DIM_SALES_ACCOUNT)
    

    call_dim_sales_account = SnowflakeOperator(
        task_id="call_dim_sales_account",
        snowflake_conn_id='snowflake_tasks_conn',
        database = target_database,
        schema='SALES',
        sql=LOAD_DIM_SALES_ACCOUNT,
        dag=dag,
        on_failure_callback=get_slack_task_failure_callback(channel))
   

    # Create an instance of SnowflakeDataMoverOperator for etl execution update
    etl_execution_update_task = SnowflakeDataMoverOperator(
        task_id="etl_execution_update_task",
        environment= environment,
        scenario='ETL_EXECUTION_UPDATE',
        etl_execution_task_id = "etl_execution_insert_task",
        payload={
            'message': 'Success',
            'execution_status_id': '2'
        },    
        dag=dag,
	on_failure_callback=get_slack_task_failure_callback(channel)
     )

    end_task = DummyOperator(task_id='end', dag=dag)    

    start_task >> etl_execution_insert_task >> [call_dim_sales_user, call_dim_account, call_dim_sales_account] >> etl_execution_update_task >> end_task
    

    