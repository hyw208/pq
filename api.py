from fastapi import FastAPI

api = FastAPI()

from pydantic import BaseModel
from typing import Any, Dict
class Payload(BaseModel):
    data: Dict[str, Any]  # A dictionary with string keys and string values

import os
from pq import PersistentQueue
jobQueue = PersistentQueue({
    "host": os.environ["DB_IP"],
    "port": os.environ["DB_PORT"],
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"]
})

@api.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

@api.post("/jobs")
async def add_job(req: Payload) -> Payload: 
    # Add a new job
    """
    curl -X POST "http://0.0.0.0:8080/jobs" -H "Content-Type: application/json" -d '{"data": {"k1":"1", "k2":"2"}}'
    """
    job_id = jobQueue.add_job(req.data)
    return {"data": {"job_id": job_id}}

@api.get("/jobs/any")
async def get_any_job(worker: str) -> Payload: 
    """
    curl -X GET "http://0.0.0.0:8080/jobs/any?worker=abc" -H "Content-Type: application/json" 
    """
    job = jobQueue.claim_next_job(worker)
    return {"data": job}

@api.put("/jobs/{job_id}")
async def update_job(job_id: str, req: Payload) -> Payload:
    """
    curl -X PUT "http://0.0.0.0:8080/jobs/1" -H "Content-Type: application/json" -d '{"data": {"worker":"abcd", "status":"success", "error": "none", "job_data": {"res": "some results"}}}'
    """
    job = jobQueue.complete_job(job_id, req.data["worker"], req.data["status"], req.data["error"], req.data["job_data"])
    return {"data": job}

