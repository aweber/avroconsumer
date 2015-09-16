avroconsumer
============
An opinionated `Rejected consumer <https://github.com/gmr/rejected>`_ class that
automatically decodes messages sent as `Avro <http://avro.apache.org/docs/1.7.7/>`_
datum.

For applications that can share schema files, Avro datum provide small, contract
based binary serialization format. Leveraging AMQP's ``Type`` message property
to convey the Avro schema file for decoding the datum, the ``DatumFileSchemaConsumer``
and ``DatumConsulSchemaConsumer`` classes extend Rejected's
``rejected.consumer.Consumer`` class adding automated deserialization of AMQP
messages serialized as Avro datums.

|Version| |Downloads| |License|

Installation
------------
avroconsumer is available on the `Python package index <https://pypi.python.org/pypi/avroconsumer>`_.

Usage
-----
avroconsumer has two base consumer types: ``DatumConsumer`` and
``DatumPublishingConsumer``. As its name implies, the ``DatumPublishingConsumer``
adds a method for publishing Avro datum as AMQP messages.

Both are consumer types are agnostic to how the Avro schema files are loaded and
both require a mixin that performs the loading of the schema file. The package
currently comes with three loader mixins:

- ``FileLoaderMixin`` loads schema files from a local directory
- ``HTTPLoaderMixin`` loads schema files from a remote directory exposed via HTTP
- ``ConsulLoaderMixin`` loads schema files from a `Consul <http://consul.io>`_ Key/Value database

FileLoaderMixin
```````````````
To use the ``FileLoaderMixin``, you need to set the ``schema_path`` config value
in the consumer configuration of the rejected configuration file. The following
snippet demonstrates an example configuration:

.. code:: yaml

  Consumers:
    apns_push:
      consumer: app.Consumer
      connections: [rabbit1]
      qty: 1
      queue: datum
      qos_prefetch: 1
      ack: True
      max_errors: 5
      config:
        schema_path: /etc/avro-schemas/

In this example configuration, if messages are published with a AMQP ``type``
message property of ``foo`` and a ``content-type`` property of
``application/vnd.apache.avro.datum``, classes extending the combination of
``DatumConsumer`` and ``HTTPLoaderMixin`` will use the Avro schema file located
at ``/etc/avro-schemas/foo.avsc`` to deserialize messages.

HTTPLoaderMixin
```````````````
The ``HTTPLoaderMixin`` loads schema files from a remote HTTP server. It expects
a config setting of ``schema_uri_format`` in the consumer configuration of the
rejected configuration file. The following snippet demonstrates an example
configuration:

.. code:: yaml

    Consumers:
      apns_push:
        consumer: app.Consumer
        connections: [rabbit1]
        qty: 1
        queue: datum
        qos_prefetch: 1
        ack: True
        max_errors: 5
        config:
          schema_uri_format: http://schema-server/avro/{0}.avsc

In this example configuration, if messages are published with a AMQP ``type``
message property of ``foo`` and a ``content-type`` property of
``application/vnd.apache.avro.datum``, classes extending the combination of
``DatumConsumer`` and ``HTTPLoaderMixin`` will use the Avro schema file located
at ``http://schema-server/avro/foo.avsc`` to deserialize messages.

ConsulLoaderMixin
`````````````````
The ``ConsulLoaderMixin`` is opinionated and loads versioned schema files
from a `consul <http://consul.io>`_ Key/Value database. The schema files should
be stored with keys in the format ``/schemas/avro/<schema-name>/<schema-version>.avsc``.
When messages are sent to consumers using the the combination of ``DatumConsumer``
and ``ConsulLoaderMixin``,the AMQP ``type`` message property should be in
``<schema-name>.<schema-version>`` format.

You can alter which Consul server is used to retrieve the schema by setting
the ``CONSUL_HOST`` and ``CONSUL_PORT`` environment variables. They default
to ``localhost`` and ``8500`` respectively.

Example
-------
The following example uses the ``DatumPublishingConsumer`` and ``HTTPLoaderMixin``
to receive a message and deserialize it. It evaluates the content of the message
and if the field ``foo`` equals ``bar`` it will publish a ``bar`` message.

.. code:: python

    class FooConsumer(avroconsumer.HTTPLoaderMixin,
                      avroconsumer.DatumPublishingConsumer):

        def process(self):

            if self.body['foo'] == 'bar':
                self.publish('bar', 'amqp.direct', 'routing-key',
                             {'timestamp': time.time()}, {'bar': True})


Enforcing Message Type
----------------------
As with any instance of ``rejected.consumer.Consumer``, the
``avroconsumer.DatumConsumer`` can automatically rejected messages based upon the
``type`` message property. Simply set the ``MESSAGE_TYPE`` attribute of your
consumer and any messages received that do not match that message type
will be rejected.

Requirements
------------
 - `avro <https://pypi.python.org/pypi/avro>`_
 - `rejected <https://pypi.python.org/pypi/rejected>`_

.. |Version| image:: https://img.shields.io/pypi/v/avroconsumer.svg?
   :target: http://badge.fury.io/py/avroconsumer

.. |Downloads| image:: https://img.shields.io/pypi/dm/avroconsumer.svg?
   :target: https://pypi.python.org/pypi/avroconsumer

.. |License| image:: https://img.shields.io/pypi/l/avroconsumer.svg?
   :target: https://avroconsumer.readthedocs.org
