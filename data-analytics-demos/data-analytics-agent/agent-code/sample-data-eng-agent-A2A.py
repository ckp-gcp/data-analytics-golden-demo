import os
import json
import logging
import uuid
import asyncio
from dotenv import load_dotenv
import httpx
import google.auth
import google.auth.transport.requests

# Load active dotenv first
load_dotenv("data_analytics_agent/.env")

# Fallbacks for debugging if not already set
if not os.getenv("GOOGLE_CLOUD_PROJECT"):
    os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "TRUE"
    os.environ["GOOGLE_CLOUD_PROJECT"] = "data-analytics-agent-i9sutj7q"
    os.environ["GOOGLE_CLOUD_LOCATION"] = "us-central1"
    os.environ["AGENT_ENV_PROJECT_ID"] = "data-analytics-agent-i9sutj7q"
    os.environ["AGENT_ENV_BIGQUERY_REGION"] = "us-central1"
    os.environ["AGENT_ENV_DATAPLEX_SEARCH_REGION"] = "global"
    os.environ["AGENT_ENV_GOOGLE_API_KEY"] = "AIzaSyDGNii0uQGuzlUUqZxZmotRGpfb9ZSjD4Q"
    os.environ["AGENT_ENV_DATAPLEX_REGION"] = "us-central1"
    os.environ["AGENT_ENV_CONVERSATIONAL_ANALYTICS_REGION"] = "global"
    os.environ["AGENT_ENV_VERTEX_AI_REGION"] = "us-central1"
    os.environ["AGENT_ENV_BUSINESS_GLOSSARY_REGION"] = "global"
    os.environ["AGENT_ENV_DATAFORM_REGION"] = "us-central1"
    os.environ["AGENT_ENV_DATAFORM_AUTHOR_NAME"] = "Adam Paternostro"
    os.environ["AGENT_ENV_DATAFORM_AUTHOR_EMAIL"] = "admin@paternostro.altostrat.com"
    os.environ["AGENT_ENV_DATAFORM_WORKSPACE_DEFAULT_NAME"] = "default"
    os.environ["AGENT_ENV_DATAFORM_SERVICE_ACCOUNT"] = "bigquery-pipeline-sa@data-analytics-agent-i9sutj7q.iam.gserviceaccount.com"
    os.environ["VERTEX_AI_ENDPOINT"] = "gemini-2.5-flash"
    os.environ["VERTEX_AI_CONNECTION_NAME"] = "vertex-ai"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def local_rest_api_helper(url: str, http_verb: str, request_body: dict = None, headers: dict = None) -> dict:
    """
    Duplicated from rest_api_helper with added support for custom headers.
    """
    print("\n--- START: local_rest_api_helper ---")
    print(f"URL: {url}")
    print(f"Verb: {http_verb}")
    
    try:
        print("Retrieving Google Cloud credentials...")
        creds, project = await asyncio.to_thread(google.auth.default)
        auth_req = google.auth.transport.requests.Request()
        print("Refreshing access token...")
        await asyncio.to_thread(creds.refresh, auth_req)
        access_token = creds.token
        print("Credentials obtained successfully.")
    except Exception as e:
        print(f"Error obtaining or refreshing Google Cloud credentials: {e}")
        raise RuntimeError(f"Authentication error: {e}")

    request_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    if headers:
        print(f"Merging custom headers: {json.dumps(headers, indent=2)}")
        request_headers.update(headers)

    print(f"Final Request Headers:\n{json.dumps(request_headers, indent=2)}")

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            http_verb = http_verb.upper()
            response = None
            print(f"Sending {http_verb} request...")
            
            if http_verb == "GET":
                response = await client.get(url, headers=request_headers)
            elif http_verb == "POST":
                response = await client.post(url, json=request_body, headers=request_headers)
            elif http_verb == "PUT":
                response = await client.put(url, json=request_body, headers=request_headers)
            elif http_verb == "PATCH":
                response = await client.patch(url, json=request_body, headers=request_headers)
            elif http_verb == "DELETE":
                response = await client.delete(url, headers=request_headers)
            else:
                raise ValueError(f"Unknown HTTP verb: {http_verb}")

            print(f"Response Status Code: {response.status_code}")
            response.raise_for_status()
            print("--- END: local_rest_api_helper (Success) ---")
            return response.json()

        except httpx.HTTPStatusError as e:
            print(f"HTTP Status Error: {e.response.status_code}")
            print(f"Response Text: {e.response.text}")
            print("--- END: local_rest_api_helper (HTTP Error) ---")
            raise RuntimeError(f"HTTP Error: {e.response.text}")
        except httpx.RequestError as e:
            print(f"Request Error: {e}")
            print("--- END: local_rest_api_helper (Request Error) ---")
            raise RuntimeError(f"Request Error: {e}")
        except Exception as e:
            print(f"Unexpected Error: {e}")
            print("--- END: local_rest_api_helper (Unexpected Error) ---")
            raise RuntimeError(f"Unexpected Error: {e}")

async def debug_call_bigquery_data_engineering_agent(repository_name: str, workspace_name: str, prompt: str) -> dict:
    """
    Duplicated and instrumented version of call_bigquery_data_engineering_agent for debugging.
    """
    print("\n=== START: debug_call_bigquery_data_engineering_agent ===")
    print(f"repository_name: {repository_name}")
    print(f"workspace_name: {workspace_name}")
    print(f"prompt:\n{prompt}\n")

    project_id = os.getenv("AGENT_ENV_PROJECT_ID")
    dataform_region = os.getenv("AGENT_ENV_DATAFORM_REGION", "us-central1")
    
    print(f"AGENT_ENV_PROJECT_ID: {project_id}")
    print(f"AGENT_ENV_DATAFORM_REGION: {dataform_region}")

    if not project_id:
        print("ERROR: AGENT_ENV_PROJECT_ID is not set. Please check your .env file or environment variables.")
        return {"status": "failed", "messages": ["AGENT_ENV_PROJECT_ID not set"]}

    messages = []

    tenant = f"projects/{project_id}/locations/{dataform_region}/agents/dataengineeringagent"
    print(f"tenant: {tenant}")
    
    url = f"https://geminidataanalytics.googleapis.com/v1/a2a/{tenant}/v1/message:send"
    print(f"url: {url}")

    gcp_resource_id = f"projects/{project_id}/locations/{dataform_region}/repositories/{repository_name}/workspaces/{workspace_name}"
    print(f"gcp_resource_id: {gcp_resource_id}")

    request_body = {
        "request": {
            "messageId": str(uuid.uuid4()),
            "role": "ROLE_USER",
            "content":[
                {
                    "text": prompt
                }
            ]
        },
        "metadata": {
            "https://geminidataanalytics.googleapis.com/a2a/extensions/gcpresource/v1": {
                "gcpResourceId": gcp_resource_id
            }
        },
        "tenant": tenant
    }
    print(f"request_body:\n{json.dumps(request_body, indent=2)}")

    headers = {
        "A2A-Extensions": "https://geminidataanalytics.googleapis.com/a2a/extensions/gcpresource/v1"
    }
    print(f"custom headers:\n{json.dumps(headers, indent=2)}")

    try:
        print("\nCalling local_rest_api_helper...")
        messages.append(f"Attempting to generate/update data engineering code in workspace '{workspace_name}' for repository '{repository_name}' with prompt: '{prompt}'.")

        # Call the local helper that supports headers
        json_result = await local_rest_api_helper(url, "POST", request_body, headers=headers)

        print("\n--- API Response Received ---")
        print(f"json_result:\n{json.dumps(json_result, indent=2)}")
        
        with open("adam_output.json", "w") as f:
            json.dump(json_result, f, indent=2)
        print("Saved response to adam_output.json")
        
        messages.append("Successfully submitted the data engineering task to the Gemini Data Analytics service.")

        print("=== END: debug_call_bigquery_data_engineering_agent (Success) ===\n")
        return {
            "status": "success",
            "tool_name": "call_bigquery_data_engineering_agent",
            "query": None,
            "messages": messages,
            "results": json_result
        }
    except Exception as e:
        print("\n--- API Error Occurred ---")
        error_message = f"An error occurred while calling the BigQuery Data Engineering agent: {e}"
        print(error_message)
        messages.append(error_message)
        print("=== END: debug_call_bigquery_data_engineering_agent (Failed) ===\n")
        return {
            "status": "failed",
            "tool_name": "call_bigquery_data_engineering_agent",
            "query": None,
            "messages": messages,
            "results": None
        }

async def main():
    # Arguments from the screenshot, using raw strings to avoid SyntaxWarnings
    repository_name = "agentic-beans-repo"
    workspace_name = "telemetry-coffee-machine-auto"
    prompt = (
        r"Correct column 'water_reservoir_level_percent' by testing for '(?:\ \ \d{2}\.\d{2}%|\ \ \d{2}\.\d{1}%\ \ )' and remove the '%' character." + "\n" +
        r"Correct column 'water_reservoir_level_percent' by testing for '0\.\d{4}' and multiply by 100." + "\n" +
        r"Correct column 'boiler_temperature_celsius' by testing for '(?:\ \ \d{2}\.\d{1}° Celsius|\ \ \d{2}\.\d{2}° Celsius\ \ )' and remove the '° Celsius' suffix."
    )

    await debug_call_bigquery_data_engineering_agent(repository_name, workspace_name, prompt)

if __name__ == "__main__":
    asyncio.run(main())
