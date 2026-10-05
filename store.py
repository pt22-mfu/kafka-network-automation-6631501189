"""Shared local job history for the API and worker."""

import sqlite3

from settings import DATABASE


def connection():
    db = sqlite3.connect(DATABASE, timeout=20)
    db.row_factory = sqlite3.Row
    return db


def initialize():
    with connection() as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                router TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                output TEXT NOT NULL DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def create(event):
    with connection() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO jobs(id, router, action, status)
            VALUES (?, ?, ?, ?)
            """,
            (event["id"], event["router"], event["action"], "queued"),
        )


def update(job_id, status, output="", pending_only=False):
    with connection() as db:
        query = "UPDATE jobs SET status=?, output=? WHERE id=?"

        if pending_only:
            query += " AND status='queued'"

        db.execute(query, (status, output, job_id))


def get(job_id):
    with connection() as db:
        row = db.execute(
            "SELECT * FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()

        return dict(row) if row else None


def recent():
    with connection() as db:
        rows = db.execute(
            """
            SELECT * FROM jobs
            ORDER BY created_at DESC, rowid DESC
            LIMIT 30
            """
        )

        return [dict(row) for row in rows]
