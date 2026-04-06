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

import os
import importlib


def make_event_node(config=None, env_prefix="EVENTNODE_"):
    """ Make *EventNode instance """
    if config is None:
        config = {}
        #
        int_vars = [
            "port",
            "callback_workers",
            "mute_first_failed_connections",
        ]
        #
        bool_vars = [
            "use_ssl",
            "ssl_verify",
        ]
        #
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                config_key = key[len(env_prefix):].lower()
                config_value = value
                #
                if config_key in int_vars:
                    config_value = int(value)
                #
                if config_key in bool_vars:
                    config_value = value.lower() in ["true", "yes"]
                #
                config[config_key] = config_value
    #
    eventnode_cfg = config.copy()
    eventnode_type = eventnode_cfg.pop("type")
    #
    eventnode_cls = getattr(
        importlib.import_module("arbiter.eventnode"),
        eventnode_type
    )
    #
    return eventnode_cls(**eventnode_cfg)
