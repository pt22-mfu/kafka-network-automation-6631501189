"""Kafka consumer -> Paramiko SSH -> GNS3 routers."""

import json
import os
import re
import sys
import time

import paramiko
from kafka import KafkaConsumer

import store
from settings import BROKERS, COMMANDS, ROUTERS, TOPIC, login


def receive(channel, prompt=None, timeout=30):
    data = ""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if channel.recv_ready():
            chunk = channel.recv(65536)

            if not chunk:
                raise ConnectionError("SSH channel closed")

            data += chunk.decode(
                "utf-8", errors="replace"
            ).replace("\r", "")

            if len(data) > 2_000_000:
                raise RuntimeError("Router output limit exceeded")

            last = data.rstrip().split("\n")[-1]

            if prompt is not None:
                if last == prompt:
                    return data, last
            elif re.fullmatch(r"[\w.():/-]+[>#]", last):
                return data, last

        elif channel.closed:
            raise ConnectionError("SSH channel closed")

        time.sleep(0.05)

    raise TimeoutError("Timed out waiting for Cisco prompt")


def execute(router, action):
    if action not in COMMANDS:
        raise ValueError("Unsupported command")

    client = paramiko.SSHClient()
    client.load_system_host_keys()

    allow_unknown = os.getenv(
        "LAB_ALLOW_UNKNOWN_HOST_KEY", "false"
    ).lower() == "true"

    if allow_unknown:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(
            **login(router),
            look_for_keys=False,
            allow_agent=False,
            timeout=10,
            auth_timeout=15,
            banner_timeout=15,
        )

        channel = client.invoke_shell(width=200, height=1000)
        _, prompt = receive(channel)

        outputs = []

        for command in ("terminal length 0", COMMANDS[action]):
            channel.sendall(command + "\n")
            output, _ = receive(channel, prompt)

            if re.search(
                r"% (Invalid|Error|Authorization|Incomplete|Ambiguous)",
                output,
            ):
                raise RuntimeError(output)

            outputs.append(output)

        return outputs[-1]

    finally:
        client.close()


def process(event):
    if (
        not isinstance(event, dict)
        or not isinstance(event.get("id"), str)
        or not event["id"]
    ):
        raise ValueError("Invalid job identifier")

    if (
        event.get("router") not in ROUTERS
        or event.get("action") not in COMMANDS
    ):
        raise ValueError("Unsupported router or command")

    store.create(event)

    if store.get(event["id"])["status"] in ("completed", "failed"):
        return

    store.update(event["id"], "running")

    try:
        output = execute(event["router"], event["action"])
        status = "completed"
    except Exception as error:
        output = f"{type(error).__name__}: {error}"
        status = "failed"

    store.update(event["id"], status, output)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--ssh-test":
        router = sys.argv[2] if len(sys.argv) > 2 else "R1"
        print(execute(router, "interfaces"))
        return

    store.initialize()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        group_id="pt-network-workers",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
    )

    print("Worker listening on " + TOPIC, flush=True)

    try:
        for message in consumer:
            try:
                event = json.loads(message.value.decode("utf-8"))
                process(event)

            except (ValueError, UnicodeError) as error:
                print(
                    f"Rejected partition {message.partition}, "
                    f"offset {message.offset}: {error}",
                    flush=True,
                )

            # Save the result before acknowledging the Kafka message.
            consumer.commit()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
