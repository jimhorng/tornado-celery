from __future__ import absolute_import

import sys

from functools import partial
from datetime import timedelta

from kombu import serialization
from kombu.utils import cached_property

from celery.app.amqp import TaskProducer
from celery.backends.amqp import AMQPBackend
from celery.backends.redis import RedisBackend
from celery.utils import timeutils

from .result import AsyncResult
try:
    from .redis import RedisConsumer
except ImportError:
    RedisConsumer = None

is_py3k = sys.version_info >= (3, 0)

import logging
_LOGGER = logging.getLogger(__name__)

class AMQPConsumer(object):
    def __init__(self, producer):
        self.producer = producer

    def wait_for(self, task_id, callback, expires=None):
        conn = self.producer.conn_pool.connection()
        conn.consume(task_id.replace('-', ''),
                     lambda *args: callback(args[3]),
                     x_expires=expires)


class NonBlockingTaskProducer(TaskProducer):

    conn_pool = None
    app = None
    result_cls = AsyncResult
    confirm_publish = False

    def __init__(self, channel=None, *args, **kwargs):
        super(NonBlockingTaskProducer, self).__init__(
            channel, *args, **kwargs)
        
        self._message_seq = 0
        self._acked = 0
        self._nacked = 0
        self._unknown_ack = 0
        self.coroutine_callbacks = {}
        if self.confirm_publish:
            conn = self.conn_pool.connection()
            conn.channel.confirm_delivery(callback=self.on_delivery_confirmation, nowait=True)

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=[], **properties):
        headers = {} if headers is None else headers
        retry_policy = {} if retry_policy is None else retry_policy
        routing_key = self.routing_key if routing_key is None else routing_key
        compression = self.compression if compression is None else compression
        exchange = exchange or self.exchange

        callback = properties.pop('callback', None)
        task_id = body['id']

        if callback and not callable(callback):
            raise ValueError('callback should be callable')
#         if callback and not isinstance(self.app.backend,
#                                        (AMQPBackend, RedisBackend)):
#             raise NotImplementedError(
#                 'callback can be used only with AMQP or Redis backends')

        body, content_type, content_encoding = self._prepare(
            body, serializer, content_type, content_encoding,
            compression, headers)

        self.serializer = self.app.backend.serializer

        serialization.registry.enable(serializer)

        (self.content_type,
         self.content_encoding,
         self.encoder) = serialization.registry._encoders[self.serializer]

        conn = self.conn_pool.connection()
        publish = conn.publish
        result = publish(body, priority=priority, content_type=content_type,
                         content_encoding=content_encoding, headers=headers,
                         properties=properties, routing_key=routing_key,
                         mandatory=mandatory, immediate=immediate,
                         exchange=exchange, declare=declare)
        
        self._message_seq += 1
        self.coroutine_callbacks[self._message_seq] = callback
        
#         if callback:
#             self.consumer.wait_for(task_id,
#                                    partial(self.on_result, task_id, callback),
#                                    self.prepare_expires(type=int))
        if not self.confirm_publish:
            callback(self.result_cls(result))
        return result

    @cached_property
    def consumer(self):
        Consumer = {
            AMQPBackend: AMQPConsumer,
            RedisBackend: RedisConsumer
        }[type(self.app.backend)]
        if not Consumer:
            raise RuntimeError(
                "tornado-redis must be installed to use the redis backend")
        return Consumer(self)

    def decode(self, payload):
        payload = is_py3k and payload or str(payload)
        return serialization.decode(payload,
                                    content_type=self.content_type,
                                    content_encoding=self.content_encoding)

    def on_result(self, task_id, callback, reply):
        reply = self.decode(reply)
        reply['task_id'] = task_id
        result = self.result_cls(**reply)
        callback(result)

    def prepare_expires(self, value=None, type=None):
        if value is None:
            value = self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(value, timedelta):
            value = timeutils.timedelta_seconds(value)
        if value is not None and type:
            return type(value * 1000)
        return value

    def __repr__(self):
        return '<NonBlockingTaskProducer: {0.channel}>'.format(self)
    
    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        delivery_tag = method_frame.method.delivery_tag
        message = ('Received %s for delivery tag: %i' %
                   (confirmation_type,
                    delivery_tag))
        _LOGGER.debug(message)
        
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        else:
            self._unknown_ack += 1
        coroutine_callback = self.coroutine_callbacks.pop(delivery_tag)
        if coroutine_callback:
            coroutine_callback(self.result_cls(None))