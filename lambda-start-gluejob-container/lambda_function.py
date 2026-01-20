import boto3
import os

def lambda_handler(event, context):
    glue = boto3.client('glue')
    job_name = os.environ.get("GLUE_JOB_NAME")

    try:
        response = glue.start_job_run(JobName=job_name)
        print(f" Glue job iniciado: {response['JobRunId']}")
        return {"status": "success", "jobRunId": response["JobRunId"]}
    except Exception as e:
        print(f" Erro ao iniciar Glue job: {e}")
        return {"status": "error", "message": str(e)}