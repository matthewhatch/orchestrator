from prefect import flow, task
from prefect.logging import get_run_logger
from tasks import get_manifest, get_versions, deploy_snowflake, deploy_prefect, deploy_salesforce_connector, publish_customer_configuration

@flow
def main_flow():
    logger = get_run_logger()

    """
    Main flow to orchestrate the deployment process.
    """

    # Load the deployment manifest
    deployment_manifest = get_manifest()

    if not deployment_manifest:
        raise ValueError("Deployment manifest is empty or not found")
    logger.info("Deployment Manifest Loaded")
    
    # Extract customers from MANIFEST
    customers = deployment_manifest.get("customers", [])
    del deployment_manifest["customers"]

    logger.info("Extracting versions from the manifest")

    for key in deployment_manifest:
        version, previous_version = get_versions(key)
        deploy(key, version, previous_version)

    for customer in customers:
        logger.info(f"Customer: {customer.get('name')}, Version: {customer.get('version', 'No version found')}")
        publish_customer_configuration(customer.get('name'), customer.get('version'), customer.get('previous', 'No previous version found'))

    logger.info("Deployment process completed successfully.")
    return deployment_manifest

@task(name="module deployment")
def deploy(module, version, previous_version):
    logger = get_run_logger()
    logger.info(f"Deploying {module}: Version: {version}, Previous Version: {previous_version}")
    if module == "snowflake":
        deploy_snowflake(version, previous_version)
    elif module == "salesforce_connector":
        deploy_salesforce_connector(version, previous_version)
    elif module == "prefect":
        deploy_prefect(version, previous_version)

if __name__ == "__main__":
    main_flow()