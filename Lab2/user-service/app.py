import json, os, uuid
from flask import Flask, redirect, url_for, render_template_string
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.environ.get("KAFKA_TOPIC", "orders")
PORT            = int(os.environ.get("PORT", "8001"))

app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

sent = []  # последние отправленные заказы для отображения

PAGE = """
<!doctype html><html><body>
<h1>user-service</h1>
<form method="post" action="/create">
  <button type="submit">Создать заказ</button>
</form>
<h2>Отправленные заказы</h2>
<table border="1" cellpadding="4">
<tr><th>order_id</th><th>товары</th><th>partition</th><th>offset</th></tr>
{% for o in sent %}
<tr><td>{{o.order_id}}</td><td>{{o['items']}}</td><td>{{o.partition}}</td><td>{{o.offset}}</td></tr>
{% endfor %}
</table>
</body></html>
"""

@app.get("/")
def index():
    return render_template_string(PAGE, sent=list(reversed(sent))[:50])

@app.post("/create")
def create():
    order = {
        "order_id": str(uuid.uuid4()),
        "items": ["apple", "banana"],
    }
    meta = producer.send(TOPIC, key=order["order_id"], value=order).get(timeout=10)
    sent.append({**order, "partition": meta.partition, "offset": meta.offset})
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)