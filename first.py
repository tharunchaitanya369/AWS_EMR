from airflow.decorators import dag, task
from datetime import datetime,timedelta
from airflow.operators.bash  import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
import boto3
from time import sleep
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

client=boto3.client("emr",region_name="us-east-1",aws_access_key_id="AKIASFZU6L3BRQSZBYG4",aws_secret_access_key="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")

default_args={"retries":2,"retry_delay":timedelta(minutes=2)}
with DAG (
    dag_id='aws_emr_pipeline',
    description="aws emr pipeline ",
    start_date=datetime(2024,10,14),
    schedule_interval="@once"   
) as dag:
    def create_emr_cluster(ti):
        cluster_id=client.run_job_flow(
            Name="EMR AIRFLOW",
            LogUri='s3://aws-logs-149898026691-us-east-1/elasticmapreduce', 
            ReleaseLabel='emr-7.3.0', 
            Applications=[
               {'Name': 'Hadoop'},  
               {'Name': 'Spark'}
                         ],
            Instances={
                 'InstanceGroups': [
                     {
                'Name': 'Master nodes',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',  
                'InstanceCount': 1
                  },
                 {
                'Name': 'Core nodes',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
                 }
                 ],
                 'Ec2SubnetId': 'subnet-02008dbb4a6dcedde',  
                'Ec2KeyName': 'chaitu',  
                'KeepJobFlowAliveWhenNoSteps': True,  
                'TerminationProtected': False  
                 },
            VisibleToAllUsers=True,  
            JobFlowRole='arn:aws:iam::149898026691:instance-profile/AmazonEMR-InstanceProfile-20240713T163409',  
            ServiceRole='arn:aws:iam::149898026691:role/service-role/AmazonEMR-ServiceRole-20240713T163424'
            
        )
        return cluster_id
        
        
    def add_steps(cluster_id,jar_file,step):
        response=client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name":"EMR",
                    "ActionOnFailure":'CONTINUE',
                    "HadoopJarStep":{
                        
                        "Jar":jar_file,
                        "Args":step
                    }
                }
            ]
        )
        return response["StepIds"][0]
    
    def get_status(cluster_id,step_id):
         response=client.describe_step(
         ClusterId=cluster_id,
         StepId=step_id
         )
         return response["Step"]["Status"]["State"]
     
    def wait_for_step(cluster_id,step_id):
        while True:
            status=get_status(cluster_id,step_id)
            if(status=="COMPLETED"):
                break
            else:
                sleep(40)
     
    def terminate_emr_cluster(cluster_id):
         response = client.terminate_job_flows(
         JobFlowIds=[cluster_id]
         )
    
    create_cluster=PythonOperator(
        task_id="create_cluster",
        python_callable=create_emr_cluster,
    )
    add_emr_steps=PythonOperator(
        task_id="add_emr_steps",
        python_callable=add_steps,
        op_args=['{{ ti.xcom_pull("create_cluster")["JobFlowId"]}}',"command-runner.jar",['spark-submit','--master','yarn','--deploy-mode','cluster',"s3://dataengineerp/input/tran1.py"] ]
    )
    get_status_emr=PythonOperator(
        task_id="get_status_emr",
        python_callable=get_status,
        op_args=['{{ ti.xcom_pull("create_cluster")["JobFlowId"]}}','{{ti.xcom_pull("add_emr_steps")}}']
    )
    wait_status_emr=PythonOperator(
        task_id="wait_status_emr",
        python_callable=wait_for_step,
        op_args=['{{ti.xcom_pull("create_cluster")["JobFlowId"]}}','{{ti.xcom_pull("add_emr_steps")}}']
    )
    terminate=PythonOperator(
        task_id="terminate",
        python_callable=terminate_emr_cluster,
        op_args=['{{ti.xcom_pull("create_cluster")["JobFlowId"]}}']
    )
    snowflake_ext=SnowflakeOperator(
		task_id="snowflake_ext",
		sql="""ALTER EXTERNAL TABLE s3_to_snowflake.PUBLIC.Iris_dataset REFRESH""" ,
		snowflake_conn_id="snowflake_conn"
	)
    create_cluster>>add_emr_steps>>get_status_emr>>wait_status_emr>>terminate>> snowflake_ext
                               

        

       