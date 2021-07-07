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
"""Handle all things session related between Kafka and ReportPortal."""
import json
import os
from requests.exceptions import ConnectionError, HTTPError
import shutil
import time


def timestamp():
    """Timestamp helper func."""
    return str(int(time.time() * 1000))


class SessionData:
    """Provide a struct for the datastore."""

    def __init__(self):
        """Do nothing but wait for data."""
        self.testnodes = {}
        pass


class Session:
    def __init__(self, sessionid, datastore=None,
                 topic=None, offset_start=None, restore=False):
        print('Session super class')
        self.data = SessionData()
        self._sessionid = sessionid
        print(f'Session.sessionid: {self.sessionid}')
        self._datastore = datastore
        print(f'Session.datastore: {self.datastore}')
        self._sessiondir = None
        self._sessionfile = None
        #self._sessiondir = f'{datastore}/{sessionid}'
        #self._sessionfile = f'{datastore}/{sessionid}.datastore'
        self.in_progress = False

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

        # temporary until this class handles the bridging
        self.bridge = None

    @property
    def sessionid(self):
        return self._sessionid

    @sessionid.setter
    def sessionid(self, sessionid):
        self._sessionid = sessionid
        self.data.sessionid = sessionid
        self.write(file='sessionid', data=sessionid)

    @property
    def datastore(self):
        return self._datastore

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
    def sessiondir(self):
        """Get the fqpath of the session directory."""
        if self._sessiondir is None:
            self.sessiondir = f'{self.datastore}/{self.sessionid}'

        return self._sessiondir

    @sessiondir.setter
    def sessiondir(self, dirpath):
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        self._sessiondir = dirpath

    @property
    def sessionfile(self):
        if self._sessionfile is None:
            self._sessionfile = f'{self.datastore}/{self.sessionid}.datastore'

        return self._sessionfile

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
        self.write(file='testnodes', data=self.data.testnodes)

    @property
    def summary(self, summary):
        """Summary of test results."""
        return self.data.summary

    @summary.setter
    def summary(self, summary):
        self.data.summary = summary
        self.write(summary=True)

    # The event handler methods
    def handle_event(self, event):
        print("SESSION super handling event:")
        #print(event.packet)

        print(f'SESSION super: kafkavent session {event.sessionid} '
              f'packet # {event.packetnum} type {event.event_type}')
        print(f'{event.body}')

        if event.event_type == 'sessionstart':
            self.start(event)
        if event.event_type == 'sessionend':
            self.end(event)
        if event.event_type == 'testresult':
            self.testresult(event)
        if event.event_type == 'summary':
            self.summary(event)

    def start(self, event):
        print('SESSION.START')
        with open(f'{self.datastore}/session.inprogress', 'w') as sfh:
            json.dump(self.sessionid, sfh)
        self.in_progress = True
        self.offset_start = event.offset

    def end(self, event):
        print('SESSION.END')
        if not self.in_progress:
            return None

        # Write session.datastore file for posterity
        self.write(summary=True)
        # Delete the session directory
        #shutil.rmtree(self.sessiondir)
        session_inprog_file = f'{self.datastore}/session.inprogress'
        if os.path.exists(session_inprog_file):
            os.unlink(session_inprog_file)

        self.offset_end = event.offset
        self.in_progress = False

    def testresult(self, event):
        print('SESSION.TESTRESULT')
        if not self.in_progress:
            return None
        self.offset_last = event.offset

    @staticmethod
    def read_offsets(sessionid, datastore):
        """Read the session offsets from the datastore."""
        # TODO: refactor this!!!
        with open(f'{datastore}/{sessionid}.datastore') as dsfh:
            session_data = json.load(dsfh)
            start_offset = session_data['offset_start']
            end_offset = session_data['offset_end']
            launchid = session_data['launch']

            return start_offset, end_offset, launchid

    # The session store methods
    def write(self, file=None, data=None, summary=True):
        """Write the session to disk (or other future)."""
        print(f'SESSION.write: file {file} = {data}')
        # TODO: make summary False when dirs are working
        # TODO: make this a lot more efficient than writing all data
        print(f'Session.write: in_progress {self.in_progress} file {file} data {data}')
        if self.in_progress and file is not None and data is not None:
            data_filename = f'{self.sessiondir}/{file}'
            if not os.path.exists(self.sessiondir):
                os.makedirs(self.sessiondir)
            with open(data_filename, "w") as chf:
                json.dump(data, chf, indent=2, sort_keys=True)

        if summary:
            print(f'SESSION.write summary file {self.sessionfile}')
            with open(self.sessionfile, "w") as ch:
                json.dump(self.data.__dict__, ch, indent=2, sort_keys=True)

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


class ReportPortalSession(Session):
    def __init__(self, sessionid, datastore=None,
                 topic=None, offset_start=None, restore=False):
        super().__init__(sessionid, datastore, topic, offset_start, restore)
        print('ReportPortalSession subclassing Session super')
        print(self._sessionid)
    '''
    def start(self, event):
        print('REPORTPORTALSESSION.START')
        
        self.node_paths = {}
        self.suite_stack = []

        # Start a KV Bridge Session
        # TODO: move RPBridge to RPSession
        self.bridge = RPBridge(self.sessionid,
                               topic=None,
                               offset_start=event.offset,
                               datastore=self.datastore)

        launch_name = event.body.get('name')
        print(f"Starting launch: {launch_name}")
        # Start launch
        attributes = [{'key': 'sessionid',
                       'value': self.sessionid}]
        description = 'Created by the bridge'
        success = False
        while not success:
            try:
                self.bridge.launch = \
                    self.service.start_launch(name=launch_name,
                                              start_time=timestamp(),
                                              description=description,
                                              rerun=False,
                                              rerunOf=None,
                                              attributes=attributes)
                success = True
            except HTTPError as err:
                print(f'ERROR: {err} '
                        '\nRetrying in 300 seconds')
                time.sleep(300)
            except ConnectionError as err:
                print(f'ERROR: {err}'
                        '\nRetrying in 300 seconds')
                time.sleep(300)

        self.kv['launch'] = self.bridge.launch
        # TODO: configurize description ^^^
        print('LAUNCH: {}', self.bridge.launch)
        self.session_in_progress = True
        self.bridge.start()
        self.bridge.offset_last = message_offset
    '''

    def summary(self, event):
        print('REPORTPORTALSESSION.SUMMARY')
        self.bridge.summary = event.body.data
        self.bridge.offset_end = event.offset
        self.bridge.offset_last = event.offset
        print(f'SUMMARY: {event.body.data}')
        print(f'OFFSET: {event.offset}')
