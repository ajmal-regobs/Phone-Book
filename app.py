import os

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request

app = Flask(__name__)


def get_contacts_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )


def get_logs_conn():
    return psycopg2.connect(
        host=os.getenv("LOGS_POSTGRES_HOST"),
        port=os.getenv("LOGS_POSTGRES_PORT", "5432"),
        user=os.getenv("LOGS_POSTGRES_USER"),
        password=os.getenv("LOGS_POSTGRES_PASSWORD"),
        dbname=os.getenv("LOGS_POSTGRES_DB"),
    )


def init_db():
    with get_contacts_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT NOT NULL UNIQUE
            )
            """
        )
    with get_logs_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/contacts")
def add_contact():
    data = request.get_json(force=True)
    name, phone = data.get("name"), data.get("phone")
    if not name or not phone:
        return jsonify({"error": "name and phone are required"}), 400
    with get_contacts_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO contacts (name, phone) VALUES (%s, %s) "
            "ON CONFLICT (phone) DO UPDATE SET name = EXCLUDED.name RETURNING *",
            (name, phone),
        )
        return jsonify(cur.fetchone()), 201


@app.delete("/contacts/<int:contact_id>")
def remove_contact(contact_id):
    with get_contacts_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))
        if cur.rowcount == 0:
            return jsonify({"error": "not found"}), 404
        return jsonify({"deleted": contact_id}), 200


@app.get("/contacts")
def list_contacts():
    with get_contacts_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, phone FROM contacts ORDER BY id")
        return jsonify(cur.fetchall())


@app.post("/logs")
def add_log():
    data = request.get_json(force=True)
    level, message = data.get("level", "info"), data.get("message")
    if not message:
        return jsonify({"error": "message is required"}), 400
    with get_logs_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO logs (level, message) VALUES (%s, %s) RETURNING *",
            (level, message),
        )
        return jsonify(cur.fetchone()), 201


@app.delete("/logs/<int:log_id>")
def remove_log(log_id):
    with get_logs_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM logs WHERE id = %s", (log_id,))
        if cur.rowcount == 0:
            return jsonify({"error": "not found"}), 404
        return jsonify({"deleted": log_id}), 200


@app.get("/logs")
def list_logs():
    with get_logs_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, level, message, created_at FROM logs ORDER BY id")
        return jsonify(cur.fetchall())


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))