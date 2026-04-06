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

from arbiter import log

from .base import EventNodeBase
from ..tools.pylon import is_runtime_gevent


class RedisEventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (redis) - allows to subscribe to events and to emit new events """

    def __init__(
            self, host, port=6379, password=None, event_queue="events",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            use_ssl=False, ssl_verify=False,
            log_errors=True,
            retry_interval=3.0,
            use_managed_identity=False,
            username=None,
    ):  # pylint: disable=R0913,R0914
        super().__init__(hmac_key, hmac_digest, callback_workers, log_errors)
        #
        self.clone_config = {
            "type": "RedisEventNode",
            "host": host,
            "port": port,
            "password": password,
            "event_queue": event_queue,
            "hmac_key": hmac_key,
            "hmac_digest": hmac_digest,
            "callback_workers": callback_workers,
            "mute_first_failed_connections": mute_first_failed_connections,
            "use_ssl": use_ssl,
            "ssl_verify": ssl_verify,
            "log_errors": log_errors,
            "retry_interval": retry_interval,
            "use_managed_identity": use_managed_identity,
            "username": username,
        }
        #
        self.retry_interval = retry_interval
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        is_gevent = is_runtime_gevent()
        #
        self.redis_event_queue = event_queue
        self.redis_config = {
            "host": host,
            "port": port,
            #
            "health_check_interval": 10,
            "max_connections": 300,  # Get from config?
            "timeout": 60,
            #
            "socket_keepalive": True,
        }
        #
        if use_managed_identity:
            from redis_entraid.cred_provider import create_from_default_azure_credential  # pylint: disable=C0415,E0401,W0401
            #
            credential_provider = create_from_default_azure_credential(  # pylint: disable=E0602
                ("https://redis.azure.com/.default",),
            )
            #
            self.redis_config["credential_provider"] = credential_provider
        else:
            if password is not None:
                self.redis_config["password"] = password
            #
            if username is not None:
                self.redis_config["username"] = username
        #
        if use_ssl:
            from redis.connection import SSLConnection  # pylint: disable=C0415,E0401
            #
            self.redis_config["connection_class"] = SSLConnection
            self.redis_config["ssl_cert_reqs"] = "required" if ssl_verify else "none"
            self.redis_config["ssl_check_hostname"] = ssl_verify
        #
        if is_gevent:
            from gevent.queue import LifoQueue  # pylint: disable=C0415,E0401
            #
            self.redis_config["queue_class"] = LifoQueue
        #
        self.redis = None
        self.redis_pool = None

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        self.redis, self.redis_pool = self._get_connection_and_pool()
        #
        super().start(emit_only)

    def stop(self):
        """ Stop event node """
        super().stop()
        #
        if self.started:
            self.redis.close()
            #
            if self.redis_pool is not None:
                self.redis_pool.close()

    def emit_data(self, data):
        """ Emit event data """
        self.redis.publish(self.redis_event_queue, data)

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """
        while self.running:
            try:
                pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.redis_event_queue)
                #
                self.listening_ready_event.set()
                #
                for message in pubsub.listen():
                    if not message:
                        continue
                    #
                    self.sync_queue.put(message.get("data", b""))
            except:  # pylint: disable=W0702
                if self.running and self.log_errors:
                    log.exception(
                        "Exception in listening thread. Retrying in %s seconds", self.retry_interval
                    )
                #
                try:
                    pubsub.close()
                except:  # pylint: disable=W0702
                    pass
                #
                if self.running:
                    time.sleep(self.retry_interval)
            finally:
                try:
                    pubsub.close()  # TODO: handle redis errors
                except:  # pylint: disable=W0702
                    pass

    def _get_connection_and_pool(self):
        while self.running:
            try:
                from redis.connection import BlockingConnectionPool  # pylint: disable=C0415,E0401
                from redis import Redis  # pylint: disable=C0415,E0401
                #
                redis_pool = BlockingConnectionPool(**self.redis_config)
                redis = Redis(connection_pool=redis_pool)
                #
                return redis, redis_pool
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
        return None, None
