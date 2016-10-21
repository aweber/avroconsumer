"""
Rejected Consumers for automatic deserialization (and serialization) of
Avro datum in RabbitMQ messages.

"""
import io
import json
from os import path

from rejected import consumer
import fastavro
from tornado import httpclient

DATUM_MIME_TYPE = 'application/vnd.apache.avro.datum'

__version__ = '1.0.0'


class Consumer(consumer.SmartPublishingConsumer):
    """Automatically deserialize Avro datum from RabbitMQ messages that have
    the ``content-type`` of ``application/vnd.apache.avro.datum``.

    """
    def __init__(self, *args, **kwargs):
        super(Consumer, self).__init__(*args, **kwargs)
        self._avro_schemas = {}

    @property
    def body(self):
        """Return the message body, deserialized if the content-type is
        set properly.

        :rtype: any

        """
        if self._message_body:
            self.logger.debug('Returning %r', self._message_body)
            return self._message_body
        body = super(Consumer, self).body
        if self.content_type == DATUM_MIME_TYPE and self.message_type:
            self.logger.debug('Deserializing %r', body)
            self._message_body = self._deserialize(
                self._avro_schema(self.message_type), body)
        return self._message_body

    def publish_message(self, exchange, routing_key, properties, body,
                        no_serialization=False,
                        no_encoding=False):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        By default, if you pass a non-string object to the body and the
        properties have a supported content-type set, the body will be
        auto-serialized in the specified content-type.

        If the ``content_type`` property is set to
        ``application/vnd.apache.avro.datum`` and the ``message_type`` is set,
        the body will attempt to be serialized as an Avro datum,

        If the properties do not have a timestamp set, it will be set to the
        current time.

        If you specify a content-encoding in the properties and the encoding is
        supported, the body will be auto-encoded.

        Both of these behaviors can be disabled by setting no_serialization or
        no_encoding to True.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param dict properties: The message properties
        :param mixed body: The message body to publish
        :param no_serialization: Turn off auto-serialization of the body
        :param no_encoding: Turn off auto-encoding of the body

        """
        if properties is None:
            properties = {}

        if not no_serialization and properties.get('type') and \
                properties.get('content_type') == DATUM_MIME_TYPE:
            body = self._serialize(self._avro_schema(properties['type']), body)

        super(Consumer, self).publish_message(
            exchange, routing_key, properties, body,
            no_serialization, no_encoding)

    def _avro_schema(self, message_type):
        """Return the cached Avro schema for the specified message type.

        :param str message_type: The message type to get the schema for
        :rtype: dict

        """
        if message_type not in self._avro_schemas.keys():
            self.logger.debug('Fetching %s schema', message_type)
            self._avro_schemas[message_type] = self._load_schema(message_type)
        self.logger.debug('Returning %s schema', message_type)
        return self._avro_schemas[message_type]

    def _load_schema(self, message_type=None):  # pragma: nocover
        """Return the schema

        :param str message_type: Optional message type to load the schema for
        :rtype: dict
        :raises: NotImplementedError

        """
        raise NotImplementedError

    @staticmethod
    def _deserialize(avro_schema, data):
        """Deserialize an Avro datum with the specified schema string

        :param dict avro_schema: The schema JSON snippet
        :param str data: The Avro datum to deserialize
        :rtype: dict

        """
        return fastavro.schemaless_reader(io.BytesIO(data), avro_schema)

    @staticmethod
    def _serialize(avro_schema, data):
        """Serialize a data structure into an Avro datum

        :param dict avro_schema: The parsed Avro schema
        :param dict data: The value to turn into an Avro datum
        :rtype: str

        """
        stream = io.BytesIO()
        fastavro.schemaless_writer(stream, avro_schema, data)
        return stream.getvalue()


class LocalSchemaConsumer(Consumer):
    """Consumer that loads schema files from disk. The schema file path is
    comprised of the ``schema_path`` configuration setting and the
    message type, appending the file type ``.avsc`` to the the end.

    """
    def initialize(self):
        """Ensure the schema_path is set in the settings"""
        self.require_setting('schema_path',
                             'avroconsumer.LocalSchemaConsumer')
        self.settings['schema_path'] = path.normpath(
            self.settings['schema_path'])
        if not path.exists(self.settings['schema_path']) or \
                not path.isdir(self.settings['schema_path']):
            raise RuntimeError(
                'schema_path {!r} is invalid'.format(
                    self.settings['schema_path']))
        super(LocalSchemaConsumer, self).initialize()

    def _load_schema(self, message_type=None):
        """Load the schema file from the file system, raising a
        ``rejected.consumer.ConsumerError`` if the the schema file can
        not be found. The schema file path is comprised of the
        ``schema_path`` configuration setting and the message type,
        appending the file type ``.avsc`` to the the end.

        :param str message_type: Optional message type to load the schema for
        :rtype: dict

        """
        message_type = message_type or self.message_type
        file_path = path.normpath(path.join(
            self.settings['schema_path'], '{0}.avsc'.format(message_type)))
        if not path.exists(file_path):
            raise consumer.ConsumerException(
                'Missing schema file: {0}'.format(file_path))
        with open(file_path, 'r') as handle:
            return json.load(handle)


class RemoteSchemaConsumer(Consumer):
    """Consumer class that implements Avro Datum decoding that loads Avro
    schemas from a remote URI. The URI format for requests is configured
    in the rejected configuration for the consumer with the
    ``schema_uri_format`` parameter:

    .. code:: yaml
        config:
            schema uri_format: http://schema-server/avro/{0}.avsc

    The ``{0}`` value is the placeholder for the message type value.

    """
    def __init__(self, *args, **kwargs):
        super(RemoteSchemaConsumer, self).__init__(*args, **kwargs)
        self._avro_http_client = httpclient.HTTPClient(force_instance=True)

    def initialize(self):
        self.require_setting('schema_uri_format',
                             'avroconsumer.RemoteSchemaConsumer')
        super(RemoteSchemaConsumer, self).initialize()

    def _load_schema(self, message_type=None):
        """Load the schema file from the file system, raising a
        ``rejected.consumer.ConsumerError`` if the the schema file can
        not be found. The schema file path is comprised of the
        ``schema_path`` configuration setting and the message type,
        appending the file type ``.avsc`` to the the end.

        :param str message_type: Optional message type to load the schema for
        :rtype: dict

        """
        message_type = message_type or self.message_type
        url = self._schema_url(message_type)
        self.logger.debug('Loading schema for %s from %s', message_type, url)
        try:
            response = self._avro_http_client.fetch(url)
        except httpclient.HTTPError as error:
            self.logger.error('Could not fetch Avro schema for %s (%s)',
                              message_type, error)
            raise consumer.ConsumerException('Error fetching avro schema')
        return json.loads(response.body.decode('utf-8'))

    def _schema_url(self, message_type):
        return self.settings['schema_uri_format'].format(message_type)
