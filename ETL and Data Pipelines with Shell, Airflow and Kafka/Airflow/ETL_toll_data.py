# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Lavanya',
    'start_date': days_ago(0),
    'email': ['lavanya@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task - unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -zxf /home/project/airflow/dags/tolldata.tgz',
    dag=dag,
)

# define the second task - extract_from_csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/vehicle-data.csv > \
    /home/project/airflow/dags/csv_data.csv',
    dag=dag,
)

# define the third task - extract_data_from_tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
   bash_command='cut -f5-7 /home/project/airflow/dags/tollplaza-data.tsv \
   |tr ' + r"'\t'" + ' "," > /home/project/airflow/dags/tsv_data.csv',
    dag=dag,
)

# define the fourth task - extract_data_from_fixed_width
extract_data_from_fixed_width = BashOperator( 
    task_id='extract_data_from_fixed_width',
      bash_command='cut -c59-67 /home/project/airflow/dags/payment-data.txt \
      |tr " " "," >  /home/project/airflow/dags/fixed_width_data.csv',
        dag=dag,
)

# define the fifth task - consolidate_data
consolidate_data = BashOperator (
    task_id='consolidate_data',
    bash_command='paste -d, /home/project/airflow/dags/csv_data.csv \
    /home/project/airflow/dags/tsv_data.csv \
    /home/project/airflow/dags/fixed_width_data.csv > \
    /home/project/airflow/dags/extracted_data.csv',
     dag=dag,
)
  # define the sixth task - transform_data
transform_data = BashOperator (
    task_id='transform_data',
    bash_command='cut -d"," -f4 /home/project/airflow/dags/extracted_data.csv \
    |tr "[:lower:]" "[:upper:]" > \
     /home/project/airflow/dags/transformed_data.csv',
   dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data