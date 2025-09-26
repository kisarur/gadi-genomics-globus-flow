#!/usr/bin/env python

def launch_workflow(globus_root: str, params_file_relative: str, work_directory_relative: str, seqera_compute_env_id: str, seqera_api_access_token: str = None) -> str:
    import json
    import yaml
    import requests
    import os

    API_BASE = "https://seqera.services.biocommons.org.au/api"

    # If seqera_api_access_token is not provided, use the SEQERA_API_ACCESS_TOKEN environment variable
    if seqera_api_access_token is None:
        seqera_api_access_token = os.getenv("SEQERA_API_ACCESS_TOKEN")
    
    if seqera_api_access_token is None or seqera_api_access_token == "":
        raise ValueError("Error: Seqera API access token is required but not provided or found in SEQERA_API_ACCESS_TOKEN environment variable.")

    # Convert paths relative to globus_root to absolute paths
    params_file = globus_root + params_file_relative
    work_directory = globus_root + work_directory_relative

    # Set API request headers
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {seqera_api_access_token}",
        "Content-Type": "application/json",
        "Accept-Version": "1"
    }

    # Read the params YAML file
    with open(params_file, "r") as f:
        params_raw = yaml.safe_load(f)

    # Replace placeholders for {globus_root} in the params with the actual globus_root, and convert to JSON
    params_resolved = {
        key: value.format(globus_root=globus_root) if isinstance(value, str) else value
        for key, value in params_raw.items()
    }
    params_text = json.dumps(params_resolved)

    # Construct the JSON payload
    data = {
        "launch": {
            "computeEnvId": seqera_compute_env_id,
            "pipeline": "file:/scratch/ma77/workflows/wf-human-variation/2.6.0/wf-human-variation.git",
            "workDir": work_directory,
            "configProfiles": [
                "singularity"
            ],
            "configText": "includeConfig \"/scratch/ma77/workflows/wf-human-variation/2.6.0/config/nci_gadi.config\"",
            "paramsText": params_text,
            "preRunScript": "module load nextflow/24.04.4; \nexport NXF_SINGULARITY_CACHEDIR=/scratch/ma77/workflows/wf-human-variation/2.6.0/images",
        }
    }

    # Make the POST request to launch the workflow
    api_url = f"{API_BASE}/workflow/launch"
    response = requests.post(api_url, headers=headers, data=json.dumps(data))

    # Parse the response
    if response.ok:
        json_response = response.json()
        workflow_id = json_response.get("workflowId")

        if workflow_id:
            return workflow_id
        else:
            raise ValueError("Error: workflowId not found in the response.")
    else:
        raise ValueError(f"Error: Failed to launch workflow. {response.status_code} {response.text}")

def monitor_workflow(workflow_id: str, seqera_api_access_token: str = None) -> None:
    import requests
    import time
    import os

    API_BASE = "https://seqera.services.biocommons.org.au/api"
    STATUS_POLL_INTERVAL = 5 * 60  # time (in seconds) between workflow status checks 

    # If seqera_api_access_token is not provided, use the SEQERA_API_ACCESS_TOKEN environment variable
    if seqera_api_access_token is None:
        seqera_api_access_token = os.getenv("SEQERA_API_ACCESS_TOKEN")
    
    if seqera_api_access_token is None or seqera_api_access_token == "":
        raise ValueError("Error: Seqera API access token is required but not provided or found in SEQERA_API_ACCESS_TOKEN environment variable.")

    # Set API request headers
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {seqera_api_access_token}",
        "Content-Type": "application/json",
        "Accept-Version": "1"
    }

    # Monitor the workflow status
    while(1):
        time.sleep(STATUS_POLL_INTERVAL)

        # Make the GET request to check the workflow status
        api_url = f"{API_BASE}/workflow/{workflow_id}"
        response = requests.get(api_url, headers=headers)

        # Parse the response
        if response.ok:
            json_response = response.json()
            status = json_response.get("workflow", {}).get("status")
            if status:
                if status == "SUCCEEDED":
                    break
                elif status in ["FAILED", "CANCELLED", "UNKNOWN"]:
                    raise ValueError(f"Error: Workflow failed with status '{status}'.")
            else:
                raise ValueError("Error: status not found in the response.")
        else:
            raise ValueError(f"Error: Failed to launch workflow. {response.status_code} {response.text}")


if __name__ == "__main__":
    from globus_compute_sdk import Client

    gcc = Client()

    launch_workflow_fuuid = gcc.register_function(launch_workflow)
    monitor_workflow_fuuid = gcc.register_function(monitor_workflow)
    
    print(f"launch_workflow function UUID: {launch_workflow_fuuid}")
    print(f"monitor_workflow function UUID: {monitor_workflow_fuuid}")