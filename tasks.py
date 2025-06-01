import boto3
import dotenv
import json
import os

from prefect import task
from prefect.logging import get_run_logger
from prefect.variables import get 

dotenv.load_dotenv()

S3_BUCKET = os.getenv('AWS_S3_BUCKET_NAME', 'default-bucket-name')  # Replace with your S3 bucket name
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')

def get_manifest():
    logger = get_run_logger()

    # read mainifests/1.0.0.json
    with open("manifests/latest.json", "r") as file:
        deployment_manifest = json.load(file)
    if not deployment_manifest:
        raise ValueError("Deployment manifest is empty or not found.")
    logger.info("Deployment Manifest Loaded")

    return deployment_manifest

def get_versions(module_name):
    """
    Get the version of a specific module from the manifest.
    """
    MANIFEST = get_manifest()
    del MANIFEST["customers"]  # Remove customers from the manifest for version extraction
    if module_name in MANIFEST:
        version = MANIFEST[module_name].get("version")
        previous_version = MANIFEST[module_name].get("previous", "No previous version found")
        return version, previous_version
    else:
        return f"{module_name} not found in manifest"

def deploy_snowflake(version, previous):
    logger = get_run_logger()
    logger.info(f"Deployment of Snowflake Schema Version: {version} Complete!")

def deploy_prefect(version, previous):
    logger = get_run_logger()
    logger.info(f"Deployment of Prefect Version: {version} Complete!")


def deploy_salesforce_connector(version, previous):
    logger = get_run_logger()
    logger.info(f"Deployment of Sales Force Connector Version: {version} Complete!")
    
@task(name="customer configuration deployment")
def publish_customer_configuration(customer, version, previous):
    logger = get_run_logger()
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3 = session.client('s3')
    config_file = f"customers/{customer}.json"
    s3_key = f"prod/{customer}/v_{version}.json"
    s3.upload_file(config_file, S3_BUCKET, s3_key)
    logger.info(f"Published configuration for {customer} to S3 at {s3_key}")
