import json
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
from kafka import KafkaProducer

span_exporter = OTLPSpanExporter(
    insecure=True
)

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf'))
    producer.send('staff', {'name': 'oleg'})
