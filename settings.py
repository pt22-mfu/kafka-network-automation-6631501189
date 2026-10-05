"""Lab configuration. Router credentials never enter Kafka messages."""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

BROKERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")

TOPIC = os.getenv("KAFKA_TOPIC", "router-commands")
DATABASE = os.getenv("JOBS_DB", str(ROOT / "jobs.sqlite3"))

COMMANDS = {
    "interfaces": "show ip interface brief",
    "routes": "show ip route",
    "version": "show version",
}

ROUTERS = {
    name: os.getenv(f"{name}_HOST", "")
    for name in ("R1", "R2")
}


def login(name):
    if name not in ROUTERS or not ROUTERS[name]:
        raise ValueError("Configure the selected router host in .env")

    return {
        "hostname": ROUTERS[name],
        "port": int(os.getenv(f"{name}_PORT", "22")),
        "username": os.getenv(f"{name}_USERNAME", ""),
        "password": os.getenv(f"{name}_PASSWORD", ""),
    }
