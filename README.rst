avroconsumer
============
A set of `Rejected <https://rejected.readthedocs.io/en/latest/>`_ classes that automatically
encode and decode AMQP message bodies as `Avro <http://avro.apache.org/docs/1.7.7/>`_ datums.

For applications that can share schema files, Avro datum provide small, contract based binary
serialization format. Leveraging AMQP's ``Type`` message property to convey the Avro schema
file for decoding the datum, avroconsumer's classes extend Rejected's
``rejected.consumer.SmartPublishingConsumer``.

|Version| |Downloads| |License| |Status| |Coverage|

Installation
------------
avroconsumer is available on the `Python package index <https://pypi.python.org/pypi/avroconsumer>`_.

Usage
-----
avroconsumer has two base consumer types: ``LocalSchemaConsumer`` and ``RemoteSchemaConsumer``.
The difference between the two resides in how they load the Avro schema file. The
``LocalSchemaConsumer`` loads schema files from a local directory, while the ``RemoteSchemaConsumer``
loads schema files from a remote location accessible via HTTP.

LocalSchemaConsumer
```````````````````
To use the ``LocalSchemaConsumer``, you need to set the ``schema_path`` config value in the consumer
configuration of the rejected configuration file. The following snippet demonstrates an example
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
        schema_path: /etc/avro-schemas/

In this example configuration, if messages are published with a AMQP ``type`` message property of
``foo`` and a ``content-type`` property of ``application/vnd.apache.avro.datum``, classes
extending the combination of ``LocalSchemaConsumer`` will use the Avro schema file located at
``/etc/avro-schemas/foo.avsc`` to deserialize messages.

RemoteSchemaConsumer
````````````````````
The ``RemoteSchemaConsumer`` loads schema files from a remote HTTP server. It expects the
``schema_uri_format`` setting in the consumer configuration of the rejected configuration file.
The following snippet demonstrates an example configuration:

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

In this example configuration, if messages are published with a AMQP ``type`` message property
of ``foo`` and a ``content-type`` property of ``application/vnd.apache.avro.datum``, classes
extending the combination of ``RemoteSchemaConsumer`` will use the Avro schema file located at
``http://schema-server/avro/foo.avsc`` to deserialize messages.

Example
-------
The following example uses the ``RemoteSchemaConsumer`` class to receive a message and
deserialize it. It evaluates the content of the message and if the field ``foo`` equals
``bar`` it will publish a ``bar`` message.

.. code:: python

    import avroconsumer


    class FooConsumer(avroconsumer.RemoteSchemaConsumer):

        def process(self):
            if self.body['foo'] == 'bar':
                self.publish('bar', 'amqp.direct', 'routing-key',
                             {'timestamp': time.time()}, {'bar': True})


Enforcing Message Type
----------------------
As with any instance of ``rejected.consumer.Consumer``, the avroconsumer classes
can automatically rejected messages based upon the ``type`` message property.
Simply set the ``MESSAGE_TYPE`` attribute of your consumer and any messages
received that do not match that message type will be rejected.

Requirements
------------
 - `fastavro <https://pypi.python.org/pypi/fastavro>`_
 - `rejected <https://pypi.python.org/pypi/rejected>`_

.. |Version| image:: https://img.shields.io/pypi/v/avroconsumer.svg?
   :target: https://pypi.python.org/pypi/avroconsumer

.. |Status| image:: https://img.shields.io/travis/gmr/avroconsumer.svg?
   :target: https://travis-ci.org/gmr/avroconsumer

.. |Coverage| image:: https://img.shields.io/codecov/c/github/gmr/avroconsumer.svg?
   :target: https://codecov.io/github/gmr/avroconsumer?branch=master

.. |Downloads| image:: https://img.shields.io/pypi/dm/avroconsumer.svg?
   :target: https://pypi.python.org/pypi/avroconsumer

.. |License| image:: https://img.shields.io/pypi/l/avroconsumer.svg?
   :target: https://pypi.python.org/pypi/avroconsumer
