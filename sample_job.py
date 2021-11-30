import airflow
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator



def bashcommands1():
    import subprocess
    cmd1 = "whoami"
    output1, error1 = process1.communicate()
    print(output1)

def print_user():
    import getpass
    print('USER:', getpass.getuser())
    exit(2)

def bashcommands2():
    file = datetime.now().strftime("%Y%m%d%H")
    import subprocess
    cmd1 = "whoami"
    cmd2 = "pwd"
    cmd3 = "touch /home/airflowtest2/" + file + ".txt"
    process1 = subprocess.Popen(cmd1.split(),stdout=subprocess.PIPE)
    process2 = subprocess.Popen(cmd2.split(),stdout=subprocess.PIPE)
    process3 = subprocess.Popen(cmd3.split(),stdout=subprocess.PIPE)
    output1, error1 = process1.communicate()
    output2, error2 = process2.communicate()
    output3, error3 = process3.communicate()
    print(output1)
    print(output2)
    print(output3)

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "email_on_failure": "true",
    #"queue": "worker2queue"
    "email": ["vishal.shah@jumia.com"]
}

dag = DAG(
    dag_id="sample_job",
    default_args=args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    max_active_runs=1
)
'''
kinit2 = BashOperator(
    task_id="doKinit2_w2",
    bash_command="hostname -f; whoami; kinit airflow -kt /opt/airflow/airflow.keytab; klist; /usr/hdp/3.1.0.0-78/hive/bin/hive -e \"select count(*) from product_feeds.jumia_categories_ext\" ",
    queue='w2',
    dag=dag,
    #run_as_user='airflow'
    #run_as_user='airflow-dev'
)
'''
kinit1 = BashOperator(
    task_id="doKinit1_default",
    bash_command="hostname -f; whoami; kinit airflow-dev -kt /opt/airflow/airflow-dev.keytab; klist; hive -e \"select count(*) from product_feeds.product_feeds_ci_production\" ",
    queue='w1',
    dag=dag,
    #run_as_user='airflow'
    #run_as_user='airflow-dev'
)

'''
t2 = PythonOperator(task_id='py_task2',
                    python_callable=print_user,
                    #run_as_user='randomtestuser2',
                    dag=dag)
t3 = PythonOperator(task_id='bigdata.ods.dwh',
                    python_callable=bashcommands2,
                    #run_as_user='randomtestuser1',
                    dag=dag)
'''

t2 = PythonOperator(task_id='py_task2',
                    python_callable=print_user,
                    #run_as_user='randomtestuser2',
                    dag=dag)
kinit1 >> t2