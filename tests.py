import json
import unittest

from helper import config
import mock
from pika import spec
from rejected import consumer, testing
import responses

import avroconsumer


class LocalSchemaConsumer(avroconsumer.LocalSchemaConsumer):

    def process(self):
        self.logger.info('Message: %r', self.body)


class RemoteSchemaConsumer(avroconsumer.RemoteSchemaConsumer):

    def process(self):
        self.logger.info('Message: %r', self.body)


class LocalSchemaConsumerTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(LocalSchemaConsumerTestCase, self).setUp()
        with open('./fixtures/push.apns.datum', 'rb') as handle:
            self.avro_datum = handle.read()

        with open('./fixtures/push.apns.v1.avsc') as handle:
            self.avro_schema = json.load(handle)

        with open('./fixtures/push.apns.json') as handle:
            self.json_data = json.load(handle)

    def tearDown(self):
        self.consumer._avro_schemas = {}

    def get_consumer(self):
        return LocalSchemaConsumer

    def get_settings(self):
        return {
            'schema_path': './fixtures/'
        }

    @testing.gen_test
    def test_decoded_body(self):
        yield self.process_message(
            self.avro_datum, avroconsumer.DATUM_MIME_TYPE,
            'push.apns.v1', {}, 'testing', 'push.apns')
        self.assertDictEqual(self.consumer.body, self.json_data)

    @testing.gen_test
    def test_that_missing_type_raises(self):
        with self.assertRaises(consumer.ConsumerException):
            yield self.process_message(
                self.avro_datum, avroconsumer.DATUM_MIME_TYPE,
                'push.apns.v2', {}, 'testing', 'push.apns')

    @testing.gen_test
    def test_json_content(self):
        yield self.process_message(
            json.dumps(self.json_data), 'application/json',
            'push.apns.v1', {}, 'testing', 'push.apns')
        self.assertDictEqual(self.consumer.body, self.json_data)

    @mock.patch('rejected.consumer.Consumer._get_pika_properties')
    @testing.gen_test
    def test_publishing(self, get_properties):
        properties = spec.BasicProperties(
            content_type=avroconsumer.DATUM_MIME_TYPE,
            type='push.apns.v1')
        get_properties.return_value = properties

        self.consumer.publish_message(
            'foo', 'bar', {
                'content_type': avroconsumer.DATUM_MIME_TYPE,
                'type': 'push.apns.v1'
            }, self.json_data)

        expectation = {
            'body': self.avro_datum,
            'exchange': 'foo',
            'properties': properties,
            'routing_key': 'bar'
        }
        self.channel.basic_publish.assert_called_once_with(**expectation)

    @mock.patch('rejected.consumer.Consumer._get_pika_properties')
    @testing.gen_test
    def test_publishing_passthrough(self, get_properties):
        properties = spec.BasicProperties()
        get_properties.return_value = properties
        self.consumer.publish_message('foo', 'bar', None, 'test_value')
        expectation = {
            'body': 'test_value',
            'exchange': 'foo',
            'properties': properties,
            'routing_key': 'bar'
        }
        self.channel.basic_publish.assert_called_once_with(**expectation)


class MisconfiguredLocalSchemaConsumerTestCase(unittest.TestCase):

    def test_initialization_raises(self):
        with self.assertRaises(Exception):
            LocalSchemaConsumer(
                config.Data({}), mock.Mock('rejected.process.Process'))

    def test_initialization_bad_path_raises(self):
        with self.assertRaises(RuntimeError):
            LocalSchemaConsumer(
                config.Data({'schema_path': './bogus_path'}),
                mock.Mock('rejected.process.Process'))


class MisconfiguredRemoteSchemaConsumerTestCase(unittest.TestCase):

    def test_initialization_raises(self):
        with self.assertRaises(Exception):
            RemoteSchemaConsumer(
                config.Data({}), mock.Mock('rejected.process.Process'))


class RemoteSchemaConsumerTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(RemoteSchemaConsumerTestCase, self).setUp()
        with open('./fixtures/push.apns.datum', 'rb') as handle:
            self.avro_datum = handle.read()
        with open('./fixtures/push.apns.v1.avsc') as handle:
            self.avro_schema = json.load(handle)
        with open('./fixtures/push.apns.json') as handle:
            self.json_data = json.load(handle)
        self.consumer._avro_schemas = {}

    def tearDown(self):
        self.consumer._avro_schemas = {}

    def get_consumer(self):
        return RemoteSchemaConsumer

    def get_settings(self):
        return {
            'schema_uri_format': 'http://localhost/schema/{}.avsc'
        }

    @testing.gen_test
    def test_requests_get_invoked(self):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET,
                     'http://localhost/schema/push.apns.v1.avsc',
                     body=json.dumps(self.avro_schema), status=200,
                     content_type='application/json')
            yield self.process_message(
                self.avro_datum, avroconsumer.DATUM_MIME_TYPE,
                'push.apns.v1', {}, 'testing', 'push.apns')
            self.assertEqual(rsps.calls[0].request. url,
                             'http://localhost/schema/push.apns.v1.avsc')

    @testing.gen_test
    def test_http_error_raises_consumer_exception(self):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET,
                     'http://localhost/schema/push.apns.v1.avsc',
                     body='timeout', status=500,
                     content_type='application/json')
            with self.assertRaises(consumer.ConsumerException):
                yield self.process_message(
                    self.avro_datum, avroconsumer.DATUM_MIME_TYPE,
                    'push.apns.v1', {}, 'testing', 'push.apns')
