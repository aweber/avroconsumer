"""
Rejected Consumers for automatic deserialization (and serialization) of
Avro datum in RabbitMQ messages.

"""
import json
from os import path
import StringIO

from rejected import consumer
from avro import io
from avro import schema

DATUM_MIME_TYPE = 'application/vnd.apache.avro.datum'


class DatumConsumer(consumer.Consumer):
    """Automatically deserialize Avro datum from RabbitMQ messages that have
    the ``content-type`` of ``application/vnd.apache.avro.datum``.

    """
    _schemas = dict()

    def prepare(self):
        """Ensure the schema_path is set in the settings"""
        if self.settings.get('schema_path') is None:
            raise consumer.ConsumerException('schema_path is not set')
        if not path.exists(path.normpath(self.settings.schema_path)):
            raise consumer.ConsumerException('schema_path is invalid')
        super(DatumConsumer, self).initialize()

    @property
    def body(self):
        """Return the message body, deserialized if the content-type is
        set properly.

        :rtype: any

        """
        # Return a materialized view of the body if it has been previously set
        if self._message_body:
            return self._message_body

        elif self.content_type == DATUM_MIME_TYPE:
            schema = self._get_schema(self.message_type)
            self._message_body = self._deserialize(schema, self._message.body)
        elif self.content_type.startswith('application/json'):
            self._message_body = json.loads(self._message.body)
        else:
            self._message_body = self._message.body

        return self._message_body

    @staticmethod
    def _deserialize(message_schema, data):
        """Deserialize an Avro datum with the specified schema string

        :param str message_schema: The schema JSON snippet
        :param str data: The Avro datum to deserialize
        :rtype: dict

        """
        datum_reader = io.DatumReader(schema.parse(message_schema))
        decoder = io.BinaryDecoder(StringIO.StringIO(data))
        return datum_reader.read(decoder)

    def _get_schema(self, message_type):
        """Fetch the Avro schema file from cache or the filesystem.

        :param str message_type:
        :rtype: str

        """
        if message_type not in self._schemas:
            self._schemas[message_type] = self._load_schema(message_type)
        return self._schemas[message_type]

    def _load_schema(self, message_type):
        """Load the schema file from the file system, raising a ``ValueError``
        if the the schema file can not be found. The schema file path is
        comprised of the ``schema_path`` configuration setting and the
        message type, appending the file type ``.avsc`` to the the end.

        :param str message_type: The message type to load the schema for
        :type: str

        """
        file_path = path.normpath(path.join(self.settings.schema_path,
                                            '{0}.avsc'.format(message_type)))
        if not path.exists(file_path):
            raise ValueError('Missing schema file: {0}'.format(file_path))

        fp = open(file_path, 'r')
        message_schema = fp.read()
        fp.close()
        return message_schema
