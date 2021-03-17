import json
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
from kafka import KafkaConsumer

span_exporter = OTLPSpanExporter(
    insecure=True
)

if __name__ == '__main__':
    consumer = KafkaConsumer(bootstrap_servers='kafka:9092')
    consumer.subscribe(['staff'])
    for message in consumer:
        print(message)
