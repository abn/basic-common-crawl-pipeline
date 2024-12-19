import argparse
import json

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import start_http_server

from commoncrawl import BASE_URL
from commoncrawl import CRAWL_PATH
from commoncrawl import CCDownloader
from commoncrawl import CSVIndexReader
from commoncrawl import Downloader
from commoncrawl import IndexReader
from rabbitmq import QUEUE_NAME
from rabbitmq import MessageQueueChannel
from rabbitmq import RabbitMQChannel


BATCH_SIZE = 50

batch_counter = Counter("batcher_batches", "Number of published batches")
processed_gauge = Gauge(
    "batcher_processed", "Percentage of the cluster.idx file processed"
)
filtered_urls_counter = Counter("batcher_filtered_urls", "Number of filtered URLs")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    print("Pushing batch of size", len(batch))  # noqa: T201
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    batch_counter.inc()


def process_index(
    index: IndexReader,
    channel: MessageQueueChannel,
    downloader: Downloader,
    batch_size: int,
) -> None:
    found_urls = []
    total_lines = len(index)
    processed_lines = 0
    for cdx_chunk in index:
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")
        for line in data.split("\n"):
            if line == "":
                continue
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))
            if (
                "languages" in metadata
                and "eng" in metadata["languages"]
                and metadata["status"] == "200"
            ):
                found_urls.append(
                    {
                        "surt_url": values[0],
                        "timestamp": values[1],
                        "metadata": metadata,
                    }
                )
            else:
                filtered_urls_counter.inc()

            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                found_urls = []

        processed_lines += 1
        processed_gauge.set((processed_lines / total_lines) * 100)

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(9000)
    channel = RabbitMQChannel()
    downloader = CCDownloader(f"{BASE_URL}/{CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
