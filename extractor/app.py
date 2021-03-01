import os
import uuid
import sys
import time

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
storage_account_key = os.getenv('STORAGE_ACCOUNT_KEY')
datalake_file_system = os.getenv('DATALAKE_FILE_SYSTEM', 'data')
datalake_directory = os.getenv('DATALAKE_DIRECTORY', 'teams')

span_exporter = OTLPSpanExporter(
    insecure=True
    # optional
    # endpoint:="localhost:55681",
    # credentials=ChannelCredentials(credentials),
    # headers=(("metadata", "metadata")),
)


def initialize_storage_account(storage_account_name, storage_account_key):

    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        print(e)


def list_directory_contents(file_system, directory):
    try:

        file_system_client = service_client.get_file_system_client(
            file_system=file_system)

        paths = file_system_client.get_paths(path=directory)

        # for path in paths:
        #     print(path.name + '\n')

    except Exception as e:
        print(e)

    return paths


def download_file_from_directory(file_system, directory, file):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=file_system)

        directory_client = file_system_client.get_directory_client(directory)
        local_file = open(file, 'wb')
        file_client = directory_client.get_file_client(file)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        local_file.write(downloaded_bytes)
        local_file.close()

    except Exception as e:
        print(e)


if __name__ == '__main__':
    resource = Resource.create({"service.name": "extractor"})
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    span_processor = BatchExportSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)

    # Configure the tracer to use the collector exporter
    tracer = trace.get_tracer_provider().get_tracer(__name__)

    initialize_storage_account(storage_account_name, storage_account_key)
    paths = list_directory_contents(datalake_file_system, datalake_directory)

    for path in paths:
        with tracer.start_as_current_span("download"):
            download_file_from_directory(datalake_file_system, datalake_directory, os.path.basename(path.name))
            print(os.path.basename(path.name))
            with tracer.start_as_current_span("read"):
                print("read")
