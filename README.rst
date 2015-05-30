avroconsumer
============
An opinionated `Rejected consumer <https://github.com/gmr/rejected>`_ class that
automatically decodes messages sent as `Avro <http://avro.apache.org/docs/1.7.7/>`_
datum.

For applications that can share schema files, Avro datum provide small, contract
based binary serialization format. Leveraging AMQP's ``Type`` message property
to convey the Avro schema file for decoding the datum, the ``DatumConsumer``
extends Rejected's ``rejected.consumer.SmartConsumer`` class adding automated
deserialization of AMQP messages serialized as Avro datums.

|Version| |Downloads| |License|

Installation
------------
avroconsumer is available on the `Python package index <https://pypi.python.org/pypi/avroconsumer>`_.

Usage
-----
To use the ``DatumConsumer``, first you need to set the ``schema_path`` configuration
setting in the rejected configuration file. The following snippet demonstrates
an example configuration:

```yaml
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
```

If messages are published with a AMQP ``type`` message property of ``foo`` and
a ``content-type`` property of ``application/vnd.apache.avro.datum``, the
``DatumConsumer`` will use the Avro schema file ``/etc/avro-schemas/foo.avsc``
to deserialize messages

The following example code shows how implement the ``DatumConsumer``.
```python
import avroconsumer


class MyConsumer(avroconsumer.DatumConsumer):

    def process(self):
        LOGGER.debug('Decoded avro datum data: %r', self.body)
```

As with any instance of ``rejected.consumer.Consumer``, the
``avroconsumer.DatumConsumer`` can automatically rejected messages based upon
the ``type`` message property. Simply set the ``MESSAGE_TYPE`` attribute
of your consumer and any messages received that do not match that message type
will be rejected. The following example demonstrates setting the message type:

```python
import avroconsumer


class MyConsumer(avroconsumer.DatumConsumer):

    MESSAGE_TYPE = 'foo'

    def process(self):
        LOGGER.debug('Decoded avro datum data: %r', self.body)
```

Requirements
------------
 - `avro <https://pypi.python.org/pypi/avro>`
 - `rejected <https://pypi.python.org/pypi/rejected>`

.. |Version| image:: https://img.shields.io/pypi/v/avroconsumer.svg?
   :target: http://badge.fury.io/py/avroconsumer

.. |Downloads| image:: https://img.shields.io/pypi/dm/avroconsumer.svg?
   :target: https://pypi.python.org/pypi/avroconsumer

.. |License| image:: https://img.shields.io/pypi/l/avroconsumer.svg?
   :target: https://avroconsumer.readthedocs.org
