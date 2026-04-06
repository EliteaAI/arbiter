#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0116,C0302

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
    Stream tools
"""


class StreamEmitter:  # pylint: disable=R0902,R0904
    """ Stream tools """

    def __init__(  # pylint: disable=R0913,R0914
            self, stream_node, stream_id,
    ):
        self.stream_node = stream_node
        self.stream_id = stream_id

    def chunk(self, chunk=None):
        """ Proxy """
        return self.stream_node.stream_chunk(self.stream_id, chunk)

    def oob(self, tag=None, payload=None):
        """ Proxy """
        return self.stream_node.stream_oob(self.stream_id, tag, payload)

    def end(self):
        """ Proxy """
        return self.stream_node.stream_end(self.stream_id)

    def exception(self, exception_info=None):
        """ Proxy """
        return self.stream_node.stream_exception(self.stream_id, exception_info)
