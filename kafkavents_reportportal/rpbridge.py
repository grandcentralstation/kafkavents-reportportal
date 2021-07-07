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
"""The module that handles all things Kafkavents to ReportPortal."""
import json
import shutil
import os


class RPBridgeData:
    """Provide a struct for the datastore."""

    def __init__(self):
        """Do nothing but wait for data."""
        self.testnodes = {}
        pass


class RPBridge():
    """The Kafkavents bridge class managing ReportPortal sessions."""

    def __init__(self, sessionid, datastore='/tmp/kafkavents/datastore',
                 topic=None, offset_start=None, restore=False):
        """Initialize the bridge."""
        self.data = RPBridgeData()
        self.datastore = datastore
        self._datastore_file = None
        self._datastore_dir = None
        self.session_in_progress = False

        if restore:
            self.resume(sessionid=sessionid, datastore=datastore,
                        topic=topic)
        else:
            self.sessionid = sessionid
            self.topic = topic
            self.offset_start = offset_start
            self.offset_end = None
            self.offset_last = None
            self.launch = None
            self.testnodes = {'None': None}
    '''
    @property
    def datastore_dir(self):
        """Directory name for the datastore."""
        if self._datastore_dir is None:
            self._datastore_dir = f'{self.datastore}/{self.sessionid}'
        return self._datastore_dir

    @property
    def datastore_file(self):
        """Filename for the datastore."""
        # FIXME: deprecate this when datastore_dir is working
        if self._datastore_file is None:
            self._datastore_file = \
                f'{self.datastore}/{self.sessionid}.datastore'
        return self._datastore_file

    @datastore_file.setter
    def datastore_file(self, datastore_file):
        self._datastore_file = datastore_file
    '''
    @property
    def sessionid(self):
        """Session ID for KV to RP."""
        return self.data.sessionid

    @sessionid.setter
    def sessionid(self, sessionid):
        self.data.sessionid = sessionid
        #self.write(file='sessionid', data=sessionid)
    '''
    @property
    def launch(self):
        """RP Launch for this session."""
        return self.data.launch

    @launch.setter
    def launch(self, launch):
        self.data.launch = launch
        self.write(file='launch', data=launch)

    @property
    def offset_end(self):
        """Kafka offset at end of session."""
        return self.data.offset_end

    @offset_end.setter
    def offset_end(self, offset):
        self.data.offset_end = offset
        self.write(file='offset_end', data=offset)

    @property
    def offset_last(self):
        """Kafka offset last read."""
        return self.data.offset_last

    @offset_last.setter
    def offset_last(self, offset):
        self.data.offset_last = offset
        self.write(file='offset_last', data=offset)

    @property
    def offset_start(self):
        """Kafka offset at start of session."""
        return self.data.offset_start

    @offset_start.setter
    def offset_start(self, offset):
        self.data.offset_start = offset
        self.write(file='offset_start', data=offset)

    @property
    def testnodes(self):
        """Test paths provided by PyTest nodeid in the Kafka event."""
        return self.data.testnodes

    @testnodes.setter
    def testnodes(self, testnodes):
        print(f'TESTNODES: {testnodes}')
        for key, value in testnodes.items():
            if key == 'None':
                self.data.testnodes = {}
            else:
                self.data.testnodes[key] = value
        #self.write(file='testnodes', data=self.data.testnodes)

    @property
    def summary(self, summary):
        """Summary of test results."""
        return self.data.summary

    @summary.setter
    def summary(self, summary):
        self.data.summary = summary
        self.write(summary=True)

    def create_session_store(self):
        """Create the session directory for storing data."""
        self.datastore_dir = f'{self.datastore}/{self.sessionid}'
        if not os.path.exists(self.datastore_dir):
            os.makedirs(self.datastore_dir)

    def write(self, file=None, data=None, summary=True):
        """Write the session to disk (or other future)."""
        # TODO: make summary False when dirs are working
        # TODO: make this a lot more efficient than writing all data
        if self.session_in_progress and file is not None and data is not None:
            if not os.path.exists(self.datastore_dir):
                os.makedirs(self.datastore_dir)
            data_filename = f'{self.datastore_dir}/{file}'
            with open(data_filename, "w") as chf:
                json.dump(data, chf, indent=2, sort_keys=True)

        if summary:
            with open(f'{self.datastore_file}', "w") as ch:
                json.dump(self.data.__dict__, ch, indent=2, sort_keys=True)

    @staticmethod
    def read_session_offsets(sessionid, datastore):
        """Read the session offsets from the datastore"""
        # TODO: refactor this!!!
        with open(f'{datastore}/{sessionid}.datastore') as dsfh:
            session_data = json.load(dsfh)
            start_offset = session_data['offset_start']
            end_offset = session_data['offset_end']
            launchid = session_data['launch']

            return start_offset, end_offset, launchid

    def start(self):
        """Start the session."""
        print("RPBRIDGE: STARTING THE SESSION")
        #session_file = f'{self.session_file}'
        #with open(f'{self.datastore_file}', "w") as ch:
        #    json.dump(self.data.__dict__, ch, indent=2, sort_keys=True)

        with open(f'{self.datastore}/session.inprogress', 'w') as sfh:
            json.dump(self.sessionid, sfh)
        self.session_in_progress = True


    def end(self):
        """End the session."""
        print("RPBRIDGE: ENDING THE SESSION")

        self.write(summary=True)
        shutil.rmtree(self.datastore_dir)
        session_file = f'{self.datastore}/session.inprogress'
        if os.path.exists(session_file):
            os.unlink(session_file)
        self.session_in_progress = False
 

    @staticmethod
    def recover_session(datastore):
        """Recover an incomplete session id."""
        recover_sessionid = None

        print(f'Searching for incomplete session in {datastore}...')
        session_file = f'{datastore}/session.inprogress'
        if os.path.exists(session_file):
            with open(session_file) as fh:
                recover_sessionid = json.load(fh)

        print(f'Recovering sessionid {recover_sessionid}')
        return recover_sessionid

    def resume(self, sessionid=None, datastore=None, topic=None):
        """Resume a session that was interrupted before completion."""
        # FIXME: not working yet
        print("DATA DICT BEFORE: ", self.data.__dict__)
        datastore_file = f'datastore/{sessionid}.datastore'
        if os.path.exists(datastore_file):
            with open(datastore_file) as fh:
                self.data.__dict__ = json.load(fh)
        print("DATA DICT AFTER: ", self.data.__dict__)
        print("DATA TEST: ", self.data.offset_last)

    def replay(self, sessionid, datastore=None):
        """Replay a session from start to end."""
        # TODO: replay reads from a session file
        pass
    '''

# TODO: clean up the property getter/setters
