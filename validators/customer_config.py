import json
import os
import pandas as pd
import re
from prefect import flow, task
from prefect.logging import get_run_logger

@task(name="load customer configuration")
def load_customer_configuration(file_path: str):
    """
    Load a customer configuration file from the specified path.
    """
    logger = get_run_logger()
    logger.info(f"Loading customer configuration from {file_path}")
    
    try:
        # load config file into json
        with open(os.path.join("customers", file_path), "r") as file:
            config = json.load(file)
        
        logger.info(f"Successfully loaded configuration for {config.get('name')}")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration from {file_path}: {e}")
        raise

@flow(name="validate customer configuration")
def validate_customer_configurations():
    """
    Read customer configuration files from cvustomers directory and validate them using Great Expectations.
    """
    logger = get_run_logger()
    logger.info("Starting customer configuration validation...")

    # Load customer configurations
    customer_configs = []
    for customer_file in os.listdir("customers"):
        if customer_file.endswith(".json"):
            config = load_customer_configuration(customer_file)
            customer_configs.append(config)

    if not customer_configs:
        raise ValueError("No customer configurations found.")

    # Validate each customer configuration using GE Validator and PandasExecutionEngine
    for config in customer_configs:
        df = pd.DataFrame([config])

        # Validate 'name' column exists and is not null
        if "name" not in df.columns:
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'name')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")
        if df["name"].isnull().any():
            logger.error(f"Validation failed for config: {config.get('name')} (null 'name')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")

        # Validate 'email' column exists and matches regex
        if "version" not in df.columns:
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'version')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")
        
        if df["version"].isnull().any():
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'version')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")
        
        if "created_at" not in df.columns:
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'created_at')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")

        if df["created_at"].isnull().any():
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'created_at')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")

        if "updated_at" not in df.columns:
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'updated_at')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")

        if df["updated_at"].isnull().any():
            logger.error(f"Validation failed for config: {config.get('name')} (missing 'updated_at')")
            raise ValueError(f"Validation failed for config: {config.get('name')}")


    logger.info("Customer configuration validation completed.")

