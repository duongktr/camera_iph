import logging
import traceback
from kafka import (
    KafkaProducer,
    KafkaConsumer,
    TopicPartition,
    OffsetAndMetadata,
)


DEFAULT_CONFIG = {"bootstrap_server": "localhost:9092", "topic": "event"}


class KafkaSender(object):
    def __init__(self, app=None, bootstrap_servers=None, topic=None, **opts):
        if bootstrap_servers is None:
            self.bootstrap_servers = DEFAULT_CONFIG["bootstrap_servers"]
        else:
            self.bootstrap_servers = bootstrap_servers

        if topic is None:
            self.topic_default = DEFAULT_CONFIG["topic"]
        else:
            self.topic_default = topic
        options = opts.copy()
        options["bootstrap_servers"] = self.bootstrap_servers
        self.__create_producer(**options)

    def __create_producer(self, **opts):
        self.producer = KafkaProducer(**opts)

    def sendmsg(
        self,
        msg,
        topic=None,
        partition=None,
        key=None,
        headers=None,
        timestamp_ms=None,
        timeout=60,
    ):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (optional): message value. Must be type bytes, or be
                serializable to bytes via configured value_serializer. If value
                is None, key is required and message acts as a 'delete'.
            partition (int, optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                'partitioner'.
            key (optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is None (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                 partition (but if key is None, partition is chosen randomly).
                Must be type bytes, or be serializable to bytes via configured
                key_serializer.
            headers (optional): a list of header key value pairs. List items
                are tuples of str key and bytes value.

            timestamp_ms (int, optional): epoch milliseconds (from Jan
                1 1970 UTC) to use as the message timestamp. Defaults
                to current time.

        Returns:
            True: suceess
            False: Failure
        Raises:
            KafkaTimeoutError: if unable to fetch topic metadata, or unable
                to obtain memory buffer prior to configured max_block_ms

        """
        if not topic:
            topic = self.topic_default
        future = self.producer.send(
            topic=topic,
            value=msg.encode("utf8"),
            partition=partition,
            key=key,
            headers=headers,
            timestamp_ms=timestamp_ms,
        )
        res = future.get(timeout=timeout)
        if future.is_done:
            return True
        else:
            return False, str(res)

    def close(self, timeout=None):
        """Close this producer.

        Arguments:
            timeout (float, optional): timeout in seconds to wait for completion.
        """
        self.producer.close(timeout)


class KafkaReader(object):
    def __init__(
        self, bootstrap_servers=None, topic=None, group_id=None, **options
    ):
        config = {
            "session_timeout_ms": 10000,
            "request_timeout_ms": 305000,
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
        }
        config.update(options)
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            **config,
        )
        self.topic = topic

    def _poll(self, timeout_ms, max_records):
        return self.consumer.poll(
            timeout_ms=timeout_ms, max_records=max_records
        )

    def poll(self, timeout_ms=0, max_records=500):
        """Fetch data from subcribed topics. The return value is a list of
        message. timeout_ms is the time waiting for a message
        available from kafka. If timeout_ms is 0, [] is returned
        immediately if no message is available. max_records is the
        maximum number of records returned in a function call.

        """
        res = self._poll(timeout_ms, max_records)
        records = []
        for k in res:
            records += res[k]
        return records

    def commit(self):
        """Manually commit the offset to kafka."""
        logger = logging.getLogger(__name__)
        try:
            self.consumer.commit()
        except Exception as e:
            logger.error(e)
            traceback.print_exc(limit=10)

    def _commit(self, offsets):
        """Manually commit the offset to kafka."""
        logger = logging.getLogger(__name__)
        _offsets = {}
        for k, v in offsets.items():
            p = TopicPartition(topic=self.topic, partition=k)
            o = OffsetAndMetadata(offset=v, metadata="")
            _offsets[p] = o
        try:
            self.consumer.commit(_offsets)
            logger.info("Kafka commited.")
        except Exception as e:
            logger.error(e)
            traceback.print_exc(limit=10)

    def _commit_next_offsets(self):
        logger = logging.getLogger(__name__)
        partitions = [
            TopicPartition(topic=self.topic, partition=p)
            for p in self.consumer.partitions_for_topic(self.topic)
        ]
        offsets = {}
        print(partitions)
        for p in partitions:
            _offset = self.consumer.position(p)
            offset = OffsetAndMetadata(offset=_offset, metadata="")
            offsets[p] = offset
            print(offsets)
        try:
            self.consumer.commit(offsets)
            logger.info("Kafka commited.")
        except Exception as e:
            logger.error(e)
            traceback.print_exc(limit=10)
