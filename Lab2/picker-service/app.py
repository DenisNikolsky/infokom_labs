import json, os, threading, time
from flask import Flask, render_template_string
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.environ.get("KAFKA_TOPIC", "orders")
GROUP           = os.environ.get("KAFKA_GROUP", "picker-group")
INSTANCE_NAME   = os.environ.get("INSTANCE_NAME", "picker-1")
PORT            = int(os.environ.get("PORT", "8002"))

app = Flask(__name__)
processed = []
lock = threading.Lock()

def consume_loop():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    for msg in consumer:
        time.sleep(0.5)  # имитация сборки заказа
        with lock:
            processed.append({
                "instance": INSTANCE_NAME,
                "partition": msg.partition,
                "offset": msg.offset,
                "order_id": msg.value.get("order_id"),
            })

PAGE = """
<!doctype html><html><body>
<h1>picker-service — {{instance}}</h1>
<table border="1" cellpadding="4">
<tr><th>instance</th><th>partition</th><th>offset</th><th>order_id</th></tr>
{% for p in processed %}
<tr><td>{{p.instance}}</td><td>{{p.partition}}</td><td>{{p.offset}}</td><td>{{p.order_id}}</td></tr>
{% endfor %}
</table>
</body></html>
"""

@app.get("/")
def index():
    with lock:
        snapshot = list(reversed(processed))[:100]
    return render_template_string(PAGE, instance=INSTANCE_NAME, processed=snapshot)

if __name__ == "__main__":
    threading.Thread(target=consume_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)