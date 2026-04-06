#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0114,C0115

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


class Config:  # pylint: disable=R0902,R0903
    def __init__(self, host, port, user, password, vhost, queue, all_queue, use_ssl=False, ssl_verify=False):  # pylint: disable=C0301,R0913
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.queue = queue
        self.all = all_queue
        self.use_ssl = use_ssl
        self.ssl_verify = ssl_verify
