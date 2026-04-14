import os

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request

app = Flask(__name__)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )


def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT NOT NULL UNIQUE
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
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "INSERT INTO contacts (name, phone) VALUES (%s, %s) "
            "ON CONFLICT (phone) DO UPDATE SET name = EXCLUDED.name RETURNING *",
            (name, phone),
        )
        return jsonify(cur.fetchone()), 201


@app.delete("/contacts/<int:contact_id>")
def remove_contact(contact_id):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))
        if cur.rowcount == 0:
            return jsonify({"error": "not found"}), 404
        return jsonify({"deleted": contact_id}), 200


@app.get("/contacts")
def list_contacts():
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, phone FROM contacts ORDER BY id")
        return jsonify(cur.fetchall())


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))