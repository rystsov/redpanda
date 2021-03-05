# sudo pip3.7 install confluent-kafka

from confluent_kafka import Producer
import json
import time

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print(msg.offset())

print("creating producer")

producer = Producer(
    {
        "bootstrap.servers": "127.0.0.1:9092",
        "enable.idempotence": True,
        "transactional.id": "my-app-id",
        "acks": "all"
    }
)

payload = {
    "key": "key1",
    "value": "value1"
}
msg = json.dumps(payload).encode("utf-8")

print("init_transactions")
producer.init_transactions()
print("begin_transaction")
producer.begin_transaction()
print("produce")
producer.produce("topic1", key=None, value=msg, callback=acked)
print("commit")
producer.commit_transaction()
print("tx done")
