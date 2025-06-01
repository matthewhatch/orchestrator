# Orchestrator

# 🧭 Prefect Deployment Orchestrator

This project defines a **Prefect flow** that orchestrates deployment operations across multiple components and customers, using a manifest-driven approach. It supports deploying specific modules (e.g., Snowflake, Salesforce Connector, Prefect workflows) and publishing per-customer configuration changes.

## 🚀 Overview

The main orchestration logic is implemented in `main_flow()` and performs the following tasks:

1. **Loads a deployment manifest**
2. **Extracts module versions**
3. **Deploys modules (Snowflake, Prefect, Salesforce Connector)**
4. **Publishes configuration for each customer**

---

🧪 Running the Flow Locally
To test or run locally:

```bash
python deploy-flow.py
```

Run with Prefect CLI
```bash
prefect deployment run main-flow/kagr-deploy
```

🔄 Deployment Workflow Details
main_flow()
- Loads the manifest using get_manifest()
- Extracts customer entries and general deployment modules
- Retrieves version info using get_versions()
- Dispatches module deployments to deploy()
- Publishes customer configuration via publish_customer_configuration()


