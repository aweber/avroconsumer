import io
import mock
from os import path
import unittest

from rejected import data

import avroconsumer


class FileLoaderMixinTests(unittest.TestCase):

    SCHEMA_NAME = 'schema'
    SCHEMA_PATH = 'static'

    @classmethod
    def get_schema(cls):
        file_path = path.join(cls.SCHEMA_PATH,
                              '{0}.avsc'.format(cls.SCHEMA_NAME))
        fp = open(file_path, 'r')
        return fp.read()

    def test_initialize_error_with_schema_path_missing(self):
        worker = avroconsumer.FileLoaderMixin()
        worker.settings = {}
        with self.assertRaises(ValueError):
            worker.initialize()

    def test_initialize_error_with_bad_schema_path(self):
        worker = avroconsumer.FileLoaderMixin()
        worker.settings = {'schema_path': 'bad'}
        with self.assertRaises(ValueError):
            worker.initialize()

    def test_schema_loaded_correctly(self):
        worker = avroconsumer.FileLoaderMixin()
        worker.settings = {'schema_path': self.SCHEMA_PATH}
        self.assertEqual(worker._load_schema(self.SCHEMA_NAME),
                         self.get_schema())

    def test_schema_loaded_error_with_bad_schema(self):
        worker = avroconsumer.FileLoaderMixin()
        worker.settings = {'schema_path': self.SCHEMA_PATH}
        with self.assertRaises(ValueError):
            worker._load_schema('bad')


class HTTPLoaderMixinTests(unittest.TestCase):

    SCHEMA_NAME = 'schema'
    SCHEMA_URI_FORMAT = 'http://example.com/avro/schemas/{0}.avsc'

    @classmethod
    def get_schema(cls):
        file_path = path.join(cls.SCHEMA_PATH,
                              '{0}.avsc'.format(cls.SCHEMA_NAME))
        fp = open(file_path, 'r')
        return fp.read()

    def test_initialize_error_with_schema_path_missing(self):
        worker = avroconsumer.HTTPLoaderMixin()
        worker.settings = {}
        with self.assertRaises(ValueError):
            worker.initialize()


class BaseDatumConsumerTests(unittest.TestCase):

    def create_consumer(self, settings):
        instance = avroconsumer.DatumConsumer(settings, mock.Mock())
        instance.initialize()
        instance.publish_message = mock.Mock()
        instance.statsd_add_timing = mock.Mock()
        instance.set_sentry_context = mock.Mock()
        return instance

    @classmethod
    def create_message(cls, message_type, message_body):
        """Create a message instance for the consumer.

        :param str message_type: identifies the type of message to create
        :param message_body: the body of the message to create

        """
        message = mock.Mock()
        message.properties = data.Properties()
        message.properties.content_encoding = None
        message.properties.content_type = 'application/vnd.apache.avro.datum'
        message.properties.correlation_id = 'CORRELATION-ID'
        message.properties.headers = {}
        message.properties.message_id = 'MESSAGE-ID'
        message.properties.type = message_type
        message.body = cls.encode_message(message_type, message_body)
        return message

    @classmethod
    def encode_message(cls, message_type, message_body):
        """Encode a message using the specified schema.

        :param str message_type: the schema type of the message
        :param str message_body: the message to encode
        :return: The encoded message.

        """
        binary_message = io.StringIO()
        encoder = io.BinaryEncoder(binary_message)
        writer = io.DatumWriter(cls.get_schema(message_type))
        writer.write(message_body, encoder)
        return binary_message.getvalue()