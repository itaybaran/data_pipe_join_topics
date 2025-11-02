from confluent_kafka import Producer
import json
import logging

class ConfluentProducer:
    def __init__(self, topic, documents_in_batch, kafka_client_auth: dict):
        self.documents_in_batch = int(documents_in_batch)
        self.document_counter = 0
        self.kafka_client_topic_name = topic
        self.kafka_client_auth = kafka_client_auth  # <<< make sure it's passed in
        self.producer = self.connect()

    def connect(self):
        try:
            return Producer(self.sasl_conf())
        except Exception as e:
            print(json.dumps({"error_code": "-2002", "error_type": "", "error_msg": str(e)}, sort_keys=True))
            raise

    def delivery_report(self, err, msg):
        if err is not None:
            print(json.dumps({"error_code": "-2003", "error_type": "", "error_msg": str(err)}, sort_keys=True))
        # else: success – keep it quiet or log if you like

    def sasl_conf(self):
        conf = {}
        for key, val in (self.kafka_client_auth or {}).items():
            if val != "$":  # your sentinel
                conf[key] = val
        return conf

    def send(self, msg):
        try:
            payload = json.dumps(msg, ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    # Use 'callback' for widest compatibility
                    self.producer.produce(
                        self.kafka_client_topic_name,
                        value=payload,
                        callback=self.delivery_report
                    )
                    break
                except BufferError:
                    # Local queue full — service delivery callbacks to free space
                    self.producer.poll(0.1)

            # service callbacks (non-blocking)
            self.producer.poll(0)

            self.document_counter += 1
            if self.document_counter >= self.documents_in_batch:
                self.flush()
                self.document_counter = 0

            print(json.dumps(msg, sort_keys=True))
        except Exception as e:
            print(json.dumps({"error_code": "-2004", "error_type": "", "error_msg": str(e)}, sort_keys=True))
            raise

    def flush(self):
        try:
            # flush waits for all outstanding messages (bounded by internal timeouts)
            self.producer.flush(5.0)
        except Exception as e:
            print(json.dumps({"error_code": "-2005", "error_type": "", "error_msg": str(e)}, sort_keys=True))
            raise

    def close(self):
        self.flush()
