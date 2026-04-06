#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
    Event node
"""

import time
import ssl

import pika  # pylint: disable=E0401

from arbiter import log
from arbiter.config import Config

from .base import EventNodeBase


class EventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (rabbitmq) - allows to subscribe to events and to emit new events """

    def __init__(
            self, host, port, user, password, vhost="carrier", event_queue="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            ssl_context=None, ssl_server_hostname=None,
            mute_first_failed_connections=0,
            use_ssl=False, ssl_verify=False,
            log_errors=True,
            retry_interval=3.0,
    ):  # pylint: disable=R0913,R0914
        super().__init__(hmac_key, hmac_digest, callback_workers, log_errors)
        #
        self.clone_config = {
            "type": "EventNode",
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "vhost": vhost,
            "event_queue": event_queue,
            "hmac_key": hmac_key,
            "hmac_digest": hmac_digest,
            "callback_workers": callback_workers,
            "ssl_context": ssl_context,
            "ssl_server_hostname": ssl_server_hostname,
            "mute_first_failed_connections": mute_first_failed_connections,
            "use_ssl": use_ssl,
            "ssl_verify": ssl_verify,
            "log_errors": log_errors,
            "retry_interval": retry_interval,
        }
        #
        self.queue_config = Config(host, port, user, password, vhost, event_queue, all_queue=None)
        #
        self.ssl_context = ssl_context
        self.ssl_server_hostname = ssl_server_hostname
        #
        if self.ssl_context is None and use_ssl:
            self.ssl_context = ssl.create_default_context()
            if ssl_verify:
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
                self.ssl_context.check_hostname = True
                self.ssl_context.load_default_certs()
            else:
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE
            self.ssl_server_hostname = host
        #
        self.retry_interval = retry_interval
        #
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0

    def emit_data(self, data):
        """ Emit event data """
        connection = self._get_connection()
        channel = self._get_channel(connection)
        #
        channel.basic_publish(
            exchange=self.queue_config.queue,
            routing_key="",
            body=data,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        #
        connection.close()

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
        while self.running:
            try:
                connection = self._get_connection()
                channel = self._get_channel(connection)
                #
                exchange_queue = channel.queue_declare(queue="", exclusive=True)
                channel.queue_bind(
                    exchange=self.queue_config.queue,
                    queue=exchange_queue.method.queue
                )
                channel.basic_consume(
                    queue=exchange_queue.method.queue,
                    on_message_callback=self._listening_callback,
                    auto_ack=True
                )
                #
                self.listening_ready_event.set()
                #
                channel.start_consuming()
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception(
                        "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                    )
                time.sleep(self.retry_interval)
            finally:
                try:
                    connection.close()
                except:  # pylint: disable=W0702
                    pass

    def _listening_callback(self, channel, method, properties, body):
        _ = channel, method, properties
        self.sync_queue.put(body)

    def _get_connection(self):
        while self.running:
            try:
                #
                pika_ssl_options = None
                if self.ssl_context is not None:
                    pika_ssl_options = pika.SSLOptions(self.ssl_context, self.ssl_server_hostname)
                #
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.queue_config.host,
                        port=self.queue_config.port,
                        virtual_host=self.queue_config.vhost,
                        credentials=pika.PlainCredentials(
                            self.queue_config.user,
                            self.queue_config.password
                        ),
                        ssl_options=pika_ssl_options,
                    )
                )
                connection.process_data_events()
                return connection
            except:  # pylint: disable=W0702
                if self.log_errors and \
                        self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                time.sleep(self.retry_interval)
        #
        return None

    def _get_channel(self, connection):
        channel = connection.channel()
        self._prepare_channel(channel)
        return channel

    def _prepare_channel(self, channel):
        channel.exchange_declare(
            exchange=self.queue_config.queue,
            exchange_type="fanout",
            durable=True
        )
