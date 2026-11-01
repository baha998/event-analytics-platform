"""Authenticated API calls shared by the optional Airflow DAGs."""
import os
import requests

def call_api(path):
    response = requests.post("http://go-api:8080/api/auth/login", json={
        "username": os.environ["AUTH_ADMIN_USERNAME"],
        "password": os.environ["AUTH_ADMIN_PASSWORD"],
    }, timeout=10)
    response.raise_for_status()
    token = response.json()["token"]
    response = requests.post("http://go-api:8080" + path,
                             headers={"Authorization": "Bearer " + token}, timeout=300)
    response.raise_for_status()
    return response.json()
