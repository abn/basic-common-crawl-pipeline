import io
import json
import os

import trafilatura

from minio import Minio
from prometheus_client import Counter
from prometheus_client import start_http_server
from warcio.archiveiterator import WARCIterator

from commoncrawl import BASE_URL
from commoncrawl import CCDownloader
from commoncrawl import Downloader
from rabbitmq import QUEUE_NAME
from rabbitmq import rabbitmq_channel


S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "worker")

# using minio client here because boto3 is just unnecessary here
s3_client = Minio(
    endpoint=os.environ.get("AWS_S3_HOST", "127.0.0.1:9000"),
    access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
    secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    secure=False,
)
batch_counter = Counter("worker_batches", "Number of consumed batches")

# let's make sure bucket exists once
if not s3_client.bucket_exists(S3_BUCKET_NAME):
    s3_client.make_bucket(S3_BUCKET_NAME)


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))  # noqa: T201
    batch = json.loads(body)
    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        for record in WARCIterator(io.BytesIO(data)):
            if record.rec_type == "response":
                _text = trafilatura.extract(record.content_stream().read())
                # TODO: process text
                if _text:
                    # this probably needs more error handling
                    s3_client.put_object(
                        bucket_name=S3_BUCKET_NAME,
                        object_name=f"{item['metadata']['filename']}.extracted.txt",
                        data=io.BytesIO(_text.encode("utf-8")),
                        length=len(_text.encode("utf-8")),
                    )
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(9001)
    downloader = CCDownloader(BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()
