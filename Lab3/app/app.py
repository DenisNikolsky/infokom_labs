import os
import time
import psycopg2
from flask import Flask, request, redirect, render_template_string

app = Flask(__name__)

# Берем адреса из переменных окружения
# Внутри Docker-сети обе базы слушают порт 5432
PRIMARY_HOST = os.environ.get("PRIMARY_HOST", "localhost")
PRIMARY_PORT = os.environ.get("PRIMARY_PORT", "5432")
REPLICA_HOST = os.environ.get("REPLICA_HOST", "localhost")
REPLICA_PORT = os.environ.get("REPLICA_PORT", "5433")
DB_USER = os.environ.get("DB_USER", "app")
DB_PASS = os.environ.get("DB_PASS", "apppass")
DB_NAME = os.environ.get("DB_NAME", "appdb")

HTML = """
<!DOCTYPE html>
<html>
<head><title>Lab 4 App</title></head>
<body style="font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
    <h1>База данных: Lab 4</h1>
    <div style="margin-bottom: 20px;">
        <form action="/create" method="POST" style="display:inline;">
            <button type="submit" style="padding: 10px 20px; font-size: 16px;">Создать запись</button>
        </form>
        <form action="/stress" method="POST" style="display:inline;">
            <button type="submit" style="padding: 10px 20px; font-size: 16px; background-color: #ff9800;">Налить нагрузку (100 записей)</button>
        </form>
    </div>

    <h2>Записи (читаем с реплики)</h2>
    <p><i>Всего записей на реплике: {{ count }}</i></p>
    <ul>
        {% for row in notes %}
            <li>[{{ row[0] }}] {{ row[1] }} <small>({{ row[2] }})</small></li>
        {% endfor %}
    </ul>
</body>
</html>
"""


def get_db_conn(host, port):
    return psycopg2.connect(
        host=host, port=port, user=DB_USER, password=DB_PASS, dbname=DB_NAME
    )


@app.route("/")
def index():
    # Читаем с реплики
    try:
        conn = get_db_conn(REPLICA_HOST, REPLICA_PORT)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM notes;")
        count = cur.fetchone()[0]
        cur.execute("SELECT id, body, created_at FROM notes ORDER BY id DESC LIMIT 10;")
        notes = cur.fetchall()
        cur.close()
        conn.close()
        return render_template_string(HTML, count=count, notes=notes)
    except Exception as e:
        return f"<h2>Ошибка чтения с реплики:</h2><pre>{e}</pre>"


@app.route("/create", methods=["POST"])
def create():
    # Пишем в primary
    try:
        conn = get_db_conn(PRIMARY_HOST, PRIMARY_PORT)
        cur = conn.cursor()
        cur.execute("INSERT INTO notes (body) VALUES (%s);", (f"Запись {time.time()}",))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<h2>Ошибка записи в primary:</h2><pre>{e}</pre>"
    return redirect("/")


@app.route("/stress", methods=["POST"])
def stress():
    # Пишем пачку в primary
    try:
        conn = get_db_conn(PRIMARY_HOST, PRIMARY_PORT)
        cur = conn.cursor()
        for i in range(100):
            cur.execute("INSERT INTO notes (body) VALUES (%s);", (f"Нагрузка {i} ({time.time()})",))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        return f"<h2>Ошибка записи в primary:</h2><pre>{e}</pre>"
    return redirect("/")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)