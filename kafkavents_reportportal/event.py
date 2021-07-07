# Copyright 2021 Jonathan Holloway <loadtheaccumulator@gmail.com>
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software. If not, see <http://www.gnu.org/licenses/>.
#
"""Handle Kafkavents events."""
import json


class Header:
    """Kafkavents standard header section."""

    def __init__(self, data):
        """Initialize the Kafkavents header."""
        self._data = data
        self._sessionid = None
        self._packetnum = None
        self._event_type = None
        self._source = None
        self._version = None
        self._timestamp = None

    @property
    def data(self):
        """Get the header text."""
        return self._data

    @property
    def event_type(self):
        """Get the event type from the packet.

        Using event_type because type is a reserved word.
        """
        if self._event_type is None:
            self._event_type = self.data.get('type', None)

        return self._event_type

    @property
    def packetnum(self):
        """Get the packet_num from the packet."""
        if self._packetnum is None:
            self._packetnum = self.data.get('packetnum', None)

        return self._packetnum

    @property
    def sessionid(self):
        """Get the sessionid."""
        if self._sessionid is None:
            self._sessionid = self.data.get('session_id', None)

        #print(f'HEADER.SESSIONID: {self._sessionid}')

        return self._sessionid


class Body:
    """Kafkavents body section.

    This can be overriden to make it specific to an application.
    """

    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data


class Event:
    def __init__(self, event):
        self._event = event
        self._packet = json.loads(event.value())
        self._is_valid = True
        self._header = None
        self._body = None
        self._offset = None

    @property
    def packet(self):
        return self._packet

    @property
    def header(self):
        if self._header is None:
            header = self.packet.get('header', None)
            if header is not None:
                self._header = Header(header)
            else:
                self.is_valid = False
                print('ERROR: Event contains no header')

        return self._header

    @property
    def sessionid(self):
        return self.header.sessionid

    @property
    def event_type(self):
        return self.header.event_type

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, offset):
        self._offset = offset

    @property
    def packetnum(self):
        return self.header.packetnum

    @property
    def body(self):
        if self._body is None:
            body = self.packet.get('event', None)
            if body is not None:
                self._body = Body(body)
            else:
                self.is_valid = False
                print('ERROR: Event contains no body')

        return self._body

    @property
    def is_valid(self):
        return self._is_valid

    @is_valid.setter
    def is_valid(self, is_valid):
        self._is_valid = is_valid
