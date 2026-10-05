"""Frontend -> HTTP API -> Kafka."""

import json
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from pydantic import BaseModel

import store
from settings import BROKERS, COMMANDS, ROOT, ROUTERS, TOPIC


@asynccontextmanager
async def lifespan(app):
    store.initialize()

    admin = KafkaAdminClient(bootstrap_servers=BROKERS)
    try:
        try:
            admin.create_topics([
                NewTopic(
                    TOPIC,
                    num_partitions=1,
                    replication_factor=1,
                )
            ])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()

    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        acks="all",
        max_block_ms=10000,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    app.state.producer = producer

    try:
        yield
    finally:
        producer.close(timeout=10)


app = FastAPI(
    title="Kafka Network Automation",
    lifespan=lifespan,
)


class Request(BaseModel):
    router: str
    action: str


@app.get("/")
def home():
    return FileResponse(ROOT / "index.html")


@app.get("/options")
def options():
    return {
        "routers": list(ROUTERS),
        "commands": COMMANDS,
    }


@app.post("/jobs", status_code=202)
def submit(request: Request):
    if request.router not in ROUTERS or request.action not in COMMANDS:
        raise HTTPException(
            status_code=400,
            detail="Choose a supported router and command",
        )

    event = {
        "id": str(uuid4()),
        "router": request.router,
        "action": request.action,
    }
    store.create(event)

    try:
        app.state.producer.send(
            TOPIC,
            key=request.router.encode("utf-8"),
            value=event,
        ).get(timeout=15)
    except KafkaError:
        store.update(
            event["id"],
            "delivery_unknown",
            "Kafka delivery was not confirmed; check this job before retrying.",
            pending_only=True,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "id": event["id"],
                "message": "Delivery not confirmed",
            },
        )

    return store.get(event["id"])


@app.get("/jobs")
def jobs():
    return store.recent()


@app.get("/jobs/{job_id}")
def result(job_id: str):
    job = store.get(job_id)

    if job is None:
        raise HTTPException(
            status_code=404,
            detail="Job not found",
        )

    return job
