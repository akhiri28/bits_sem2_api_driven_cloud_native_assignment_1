import requests
from typing import Dict, Any

# ---------------- CONFIG ----------------
API_KEY = "pnu_ShjJmUj9bI7SqQuhgRbXl4e03Xb0dK12YpUn"
ACCOUNT_ID = "f4ec5cd6-e3c9-4219-8038-29efc8215b9c"
WORKSPACE_ID = "299e6383-aa8f-49b4-b251-683bd908ed99"

BASE_URL = f"https://api.prefect.cloud/api/accounts/f4ec5cd6-e3c9-4219-8038-29efc8215b9c/workspaces/299e6383-aa8f-49b4-b251-683bd908ed99"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}


# ---------------- To Print the Response ----------------
def post(endpoint: str, payload: Dict[str, Any] = {}):
    url = f"{BASE_URL}{endpoint}"
    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code != 200:
        print(f"Error: {response.status_code} -> {response.text}")
        return None

    return response.json()


# ---------------- FETCH FLOWS ----------------
def get_flows():
    print("\n🔹 Fetching flows...")
    data = post("/flows/filter", {})

    flow_map = {}
    if data:
        for flow in data:
            flow_map[flow["id"]] = flow["name"]
            print(f"Flow Name: {flow['name']} | ID: {flow['id']}")

    return flow_map


# ---------------- FETCH DEPLOYMENTS ----------------
def get_deployments():
    print("\n🔹 Fetching deployments...")
    data = post("/deployments/filter", {})

    deployment_map = {}
    if data:
        for dep in data:
            deployment_map[dep["id"]] = dep["name"]
            print(
                f"Deployment: {dep['name']} | "
                f"Flow ID: {dep['flow_id']} | ID: {dep['id']}"
            )

    return deployment_map


# ---------------- FETCH FLOW RUNS ----------------
def get_flow_runs(flow_map, deployment_map, limit=10):
    print("\n🔹 Fetching recent flow runs...")

    payload = {
        "sort": "START_TIME_DESC",
        "limit": limit
    }

    data = post("/flow_runs/filter", payload)

    if data:
        for run in data:
            flow_name = flow_map.get(run["flow_id"], "Unknown Flow")
            deployment_name = deployment_map.get(
                run.get("deployment_id"), "No Deployment"
            )

            print(
                f"Run: {run['name']} | "
                f"State: {run['state']['type']} | "
                f"Flow: {flow_name} | "
                f"Deployment: {deployment_name}"
            )

    return data


# ---------------- FETCH LOGS ----------------
def get_logs_for_run(flow_run_id, limit=50):
    print("\n Fetching logs...")

    payload = {
        "limit": limit,
        "sort": "TIMESTAMP_ASC",
        "logs": {
            "flow_run_id": {"any_": [flow_run_id]}
        }
    }

    data = post("/logs/filter", payload)

    if data:
        for log in data:
            print(f"[{log['timestamp']}] {log['level']} - {log['message']}")
    else:
        print("No logs found")


# ---------------- LAST COMPLETED RUN ----------------
def get_last_completed_run_logs(flow_map, deployment_map):
    print("\n🔹Fetching last completed run...")

    payload = {
        "sort": "START_TIME_DESC",
        "limit": 1,
        "flow_runs": {
            "state": {
                "type": {"any_": ["COMPLETED"]}
            }
        }
    }

    data = post("/flow_runs/filter", payload)

    if data and len(data) > 0:
        run = data[0]

        flow_name = flow_map.get(run["flow_id"], "Unknown Flow")
        deployment_name = deployment_map.get(
            run.get("deployment_id"), "No Deployment"
        )

        # PRINT RUN NAME FIRST (your requirement)
        print("\n LAST SUCCESSFUL RUN NAME:")
        print(f"{run['name']}")

        print("\n RUN DETAILS")
        print(f"Flow       : {flow_name}")
        print(f"Deployment : {deployment_name}")
        print(f"State      : {run['state']['type']}")
        print(f"Start Time : {run['start_time']}")
        print(f"End Time   : {run.get('end_time')}")

        # Fetch logs AFTER printing name
        get_logs_for_run(run["id"])

    else:
        print("No completed runs found")


# ---------------- MAIN ----------------
if __name__ == "__main__":
    flow_map = get_flows()
    deployment_map = get_deployments()
    runs = get_flow_runs(flow_map, deployment_map)

    get_last_completed_run_logs(flow_map, deployment_map)