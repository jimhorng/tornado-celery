from __future__ import absolute_import

try:
    from urlparse import urlparse
except ImportError:  # py3k
    from urllib.parse import urlparse
from functools import partial
from itertools import cycle
from datetime import timedelta

import pika
import logging

from pika.adapters.tornado_connection import TornadoConnection
from pika.exceptions import AMQPConnectionError

from tornado import ioloop


class Connection(object):

    content_type = 'application/x-python-serialize'
    additional_channel_setup = None

    def __init__(self, io_loop=None):
        self.channel = None
        self.connection = None
        self.url = None
        self.io_loop = io_loop or ioloop.IOLoop.instance()

    def connect(self, url=None, options=None, callback=None):
        if url is not None:
            self.url = url
        purl = urlparse(self.url)
        credentials = pika.PlainCredentials(purl.username, purl.password)
        virtual_host = purl.path[1:]
        host = purl.hostname
        port = purl.port

        options = options or {}
        options = dict([(k.lstrip('DEFAULT_').lower(), v) for k, v in options.items()])
        options.update(host=host, port=port, virtual_host=virtual_host,
                       credentials=credentials)

        params = pika.ConnectionParameters(**options)
        try:
            TornadoConnection(
                params, stop_ioloop_on_close=False,
                on_open_callback=partial(self.on_connect, callback),
                custom_ioloop=self.io_loop)
        except AMQPConnectionError:
            logging.info('Retrying to connect in 2 seconds')
            self.io_loop.add_timeout(
                timedelta(seconds=2),
                partial(self.connect, url=url,
                        options=options, callback=callback))

    def on_connect(self, callback, connection):
        self.connection = connection
        self.connection.add_on_close_callback(self.on_closed)
        self.connection.channel(partial(self.on_channel_open, callback))

    def on_channel_open(self, callback, channel):
        self.channel = channel
        if self.additional_channel_setup:
            self.additional_channel_setup(self.channel)
        if callback:
            callback()

    def on_exchange_declare(self, frame):
        pass

    def on_basic_cancel(self, frame):
        self.connection.close()

    def on_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        logging.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                        reply_code, reply_text)
        connection.add_timeout(5, self.connect)

    def publish(self, body, exchange=None, routing_key=None,
                mandatory=False, immediate=False, content_type=None,
                content_encoding=None, serializer=None,
                headers=None, compression=None, retry=False,
                retry_policy=None, declare=[], **properties):
        assert self.channel
        content_type = content_type or self.content_type

        properties = pika.BasicProperties(content_type=content_type)

        self.channel.basic_publish(exchange=exchange, routing_key=routing_key,
                                   body=body, properties=properties,
                                   mandatory=mandatory, immediate=immediate)

    def consume(self, queue, callback, x_expires=None):
        assert self.channel
        self.channel.queue_declare(self.on_queue_declared, queue=queue,
                                   exclusive=False, auto_delete=True,
                                   nowait=True, durable=True,
                                   arguments={'x-expires': x_expires})
        self.channel.basic_consume(callback, queue, no_ack=True)

    def on_queue_declared(self, *args, **kwargs):
        pass


class ConnectionPool(object):
    def __init__(self, limit, io_loop=None):
        self._limit = limit
        self._connections = []
        self._connection = None
        self.io_loop = io_loop

    def connect(self, broker_url, options=None, callback=None):
        self._on_ready = callback
        for _ in range(self._limit):
            conn = Connection(io_loop=self.io_loop)
            conn.connect(broker_url, options=options,
                         callback=partial(self._on_connect, conn))

    def _on_connect(self, connection):
        self._connections.append(connection)
        if len(self._connections) == self._limit:
            self._connection = cycle(self._connections)
            if self._on_ready:
                self._on_ready()

    def connection(self):
        assert self._connection is not None
        return next(self._connection)

    def connections(self):
        return self._connections