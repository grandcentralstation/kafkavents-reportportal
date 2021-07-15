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


class Sessions:
    """Handle multiple sessions."""

    def __init__(self, datastore):
        """Initialize a Sessions instance."""
        pass

    @classmethod
    def inprogress(self, datastore):
        """Leave no session behind."""
        #print(f'DATASTORE: {datastore}')
        inprogress = []
        for root, dirs, files in os.walk(datastore):
            #print(f'{root} {dirs} {files}')
            for file in files:
                #print(f'FILE: {file}')
                if file.endswith(".inprogress"):
                    fqpath = os.path.join(root, file)
                    #print(f'FQPATH: {fqpath}')
                    inprogress.append(fqpath)

        return inprogress


class SessionData:
    """Provide a struct for the datastore."""

    def __init__(self):
        """Do nothing but wait for data."""
        self.testnodes = {}
        self.offset_end = None


class Session:
    """Handle all things session base class."""

    def __init__(self, sessionid, datastore=None,
                 topic=None, offset_start=None,
                 reportportal=None, restore=False):
        """Initialize an instance as the base class."""
        #print('Session super class')
        self.reportportal = reportportal
        self.rp_service = reportportal.service
        self.data = SessionData()
        self._sessionid = sessionid
        #print(f'Session.sessionid: {self.sessionid}')
        self._datastore = datastore
        #print(f'Session.datastore: {self.datastore}')
        self._sessiondir = None
        self._sessionfile = None
        #self._sessiondir = f'{datastore}/{sessionid}'
        #self._sessionfile = f'{datastore}/{sessionid}.datastore'
        self.in_progress = False
        self.suite_stack = []

        self.restore_flag = False
        if restore:
            self.resume(sessionid=sessionid, datastore=datastore,
                        topic=topic)
            self.restore_flag = True
            self.in_progress = True
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
        """Get the sessionid."""
        return self._sessionid

    @sessionid.setter
    def sessionid(self, sessionid):
        self._sessionid = sessionid
        self.data.sessionid = sessionid
        self.write(file='sessionid', data=sessionid)

    @property
    def datastore(self):
        """Get the datastore path."""
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
        """Get the FQPATH of the session file."""
        suffix = "datastore"
        if self.offset_end is None:
            suffix = "inprogress"

        self._sessionfile = f'{self.datastore}/{self.sessionid}.{suffix}'

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
        """Handle an incoming event."""
        #print(event.packet)

        print(f'SESSION super: kafkavent session {event.sessionid} '
              f'packet # {event.packetnum} type {event.event_type}')

        if event.event_type == 'sessionstart':
            self.start_event(event)
        if event.event_type == 'sessionend':
            self.end_event(event)
        if event.event_type == 'testresult':
            self.testresult_event(event)
        #if event.event_type == 'summary':
        #    self.summary_event(event)

    def start_event(self, event):
        """Start a session."""
        print('SESSION.START')
        self.in_progress = True
        #with open(f'{self.datastore}/session.inprogress', 'w') as sfh:
        #    json.dump(self.sessionid, sfh)
        self.offset_start = event.offset

    def end_event(self, event):
        """End a session."""
        print('SESSION.END')
        if not self.in_progress:
            return None

        # Delete the session directory
        if os.path.exists(self.sessiondir):
            shutil.rmtree(self.sessiondir)
        #session_inprog_file = f'{self.datastore}/session.inprogress'
        #if os.path.exists(session_inprog_file):
        #    os.unlink(session_inprog_file)

        if os.path.exists(self.sessionfile):
            os.unlink(self.sessionfile)
        self.offset_end = event.offset
        self.in_progress = False
        # Write session.datastore file for posterity
        self.write(summary=True)

    def testresult_event(self, event):
        """Handle a testcase result."""
        print('SESSION.TESTRESULT')
        if not self.in_progress:
            return None
        self.offset_last = event.offset

    def summary_event(self, event):
        """Handle a summary event."""
        print('SESSION.SUMMARY')
        self.summary = event.body.data
        self.offset_end = event.offset
        self.offset_last = event.offset
        print(f'SUMMARY: {event.body.data}')
        print(f'OFFSET: {event.offset}')
        # Delete the session directory
        if os.path.exists(self.sessiondir):
            shutil.rmtree(self.sessiondir)

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
        #print(f'SESSION.write: file {file} = {data}')
        # TODO: make summary False when dirs are working
        # TODO: make this a lot more efficient than writing all data
        '''
        if self.in_progress and file is not None and data is not None:
            data_filename = f'{self.sessiondir}/{file}'
            if not os.path.exists(self.sessiondir):
                os.makedirs(self.sessiondir)
            with open(data_filename, "w") as chf:
                json.dump(data, chf, indent=2, sort_keys=True)
        '''
        if summary:
            #print(f'SESSION.write summary file {self.sessionfile}')
            with open(self.sessionfile, "w") as ch:
                json.dump(self.data.__dict__, ch, indent=2, sort_keys=True)

    @staticmethod
    def recover(datastore, sessionid=None):
        """Recover an incomplete session id."""
        recover_sessionid = None

        print(f'Searching for incomplete session in {datastore}...')
        inprogress = Sessions.inprogress(datastore)
        for session_file in inprogress:
            #print(f'SESSION FILE: {session_file}')
            # FIXME: this breaks if there is more than one session
            if os.path.exists(session_file):
                with open(session_file) as fh:
                    recover_session = json.load(fh)
                #print(f'RECOVER SESSION: {recover_session}')
                recover_sessionid = recover_session.get('sessionid', None)

                print(f'Recovering sessionid {recover_sessionid}')

        return recover_sessionid

    def resume(self, sessionid=None, datastore=None, topic=None):
        """Resume a session that was interrupted before completion."""
        # FIXME: not working yet
        print("DATA DICT BEFORE: ", self.data.__dict__)
        datastore_file = f'datastore/{sessionid}.inprogress'
        if os.path.exists(datastore_file):
            with open(datastore_file) as fh:
                self.data.__dict__ = json.load(fh)
        print("DATA DICT AFTER: ", self.data.__dict__)
        print("DATA TEST: ", self.data.offset_last)

    def replay(self, sessionid, datastore=None):
        """Replay a session from start to end."""
        # TODO: replay reads from a session file
        pass

    def get_parent_id(self, node_path):
        """Get the parent id for the nodepath."""
        nodetets = node_path.split('.')
        nodetets.pop()
        if len(nodetets) == 0:
            print('using launch id')
            parent_id = self.launch
        else:
            #print('looking up id')
            parent_id = self.testnodes.get('.'.join(nodetets))
        #print(f'Domain {node_path}, Parent {parent_id}')
        return parent_id

    def create_missing_testnodes(self, nodeid):
        """Create a list of node paths from a test nodeid."""
        print(f'CREATE MISSING TESTNODES.... SESSION {self.sessionid}')
        nodetets = nodeid.split('.')
        counter = 0
        while counter < len(nodetets) - 1:
            range_end = counter - len(nodetets) + 1
            #print(f'COUNTER: {counter}:{range_end}')
            node_path = ".".join(nodetets[0:range_end])

            if node_path not in self.testnodes:
                parent_id = self.get_parent_id(node_path)
                print(f'CREATING NODE {node_path} with parent {parent_id}')
                if counter == 0:
                    # RP requires parent_uuid when parent is launch
                    suite_id = \
                        self.rp_service.start_test_item(parent_uuid=parent_id,
                                                        name=nodetets[counter],
                                                        start_time=timestamp(),
                                                        item_type="SUITE")
                else:
                    suite_id = \
                        self.rp_service.start_test_item(parent_item_id=parent_id,
                                                        name=nodetets[counter],
                                                        start_time=timestamp(),
                                                        item_type="SUITE")
                self.testnodes[node_path] = suite_id
                self.suite_stack.append(suite_id)

                # a bit of a misnomer hack here.
                # uses a setter to set an entry in the dictionary
                # not the entire dictionary
                # FIXME: this should work with a direct assign
                #session.testnodes = {node_path: suite_id}
                #print(f'CREATE NODES: {session.testnodes}')
                if self.testnodes is not None:
                    self.testnodes = {node_path: suite_id}
            counter = counter + 1


class ReportPortalSession(Session):
    """Subclass the Session class for things specific to ReportPortal."""

    def __init__(self, sessionid, datastore=None,
                 topic=None, offset_start=None,
                 reportportal=None, restore=False):
        """Initialize and instance of a ReportPortal session."""
        super().__init__(sessionid, datastore, topic, offset_start,
                         reportportal, restore)
        print('ReportPortalSession subclassing Session super')
        print(self._sessionid)

    def start_event(self, event):
        """Handle the start of the session."""
        print('SESSION START')
        super().start_event(event)

        #self.node_paths = {}
        self.suite_stack = []

        launch_name = event.body.data.get('name', None)
        print(f"Starting launch: {launch_name}")
        # Start launch
        attributes = [{'key': 'sessionid',
                       'value': self.sessionid}]
        description = 'Created by kafkavents-reportportal'
        success = False
        while not success:
            try:
                launch = \
                    self.rp_service.start_launch(name=launch_name,
                                                 start_time=timestamp(),
                                                 description=description,
                                                 rerun=False,
                                                 rerunOf=None,
                                                 attributes=attributes)
                self.launch = launch
                success = True
            except HTTPError as err:
                print(f'ERROR: {err} '
                      '\nRetrying in 300 seconds')
                time.sleep(300)
            except ConnectionError as err:
                print(f'ERROR: {err}'
                      '\nRetrying in 300 seconds')
                time.sleep(300)

        # TODO: configurize description ^^^
        print('LAUNCH: {}', self.launch)
        self.in_progress = True

    def end_event(self, event):
        """Handle the end of the session."""
        #print('SESSION END')
        if not self.in_progress:
            # TODO: handle mid-session restarts
            return
        #self.offset_end = event.offset

        launch_name = event.body.data.get('name')
        print(f"Ending launch: {launch_name}")
        # Finish launch.
        self.rp_service.finish_launch(launch=self.launch,
                                      end_time=timestamp())
        #print(self.node_paths)

        self.offset_last = event.offset
        # Close the suites
        for itemid in reversed(self.suite_stack):
            self.rp_service.finish_test_item(item_id=itemid,
                                             end_time=timestamp(),
                                             status=None)
            #print(f'CLOSED: {itemid}')
        super().end_event(event)

    def testresult_event(self, event):
        """Handle test result events."""
        print('SESSION TESTRESULT')
        super().testresult_event(event)
        if not self.in_progress:
            # TODO: refactor this to the bridge.resume()
            print('WARNING: NO SESSION IN PROGRESS. '
                  'Skipping to the next SESSION END')
            return

        # check to see if we're about to dupe a testitem

        if self.restore_flag:
            # Get the int id for the launch
            url = (f'launch?'
                   f'filter.eq.uuid={self.launch}')
            data = self.reportportal.api_get(url)
            data = json.loads(data.text)
            launch_num = data['content'][0]['id']

            # check for an existing test item for offset
            url = (f'item?filter.has.attributeKey=kv_offset&'
                   f'filter.has.attributeValue={event.offset}&'
                   f'filter.eq.launchId={launch_num}')
            data = self.reportportal.api_get(url)
            data = json.loads(data.text)
            print(f'LAUNCH {launch_num} TESTITEM DATA: {data}')
            num_found = data['page']['totalElements']
            print(f'NUM_FOUND: {num_found}')

            # clear the flag and continue if about to dupe
            self.restore_flag = False
            if num_found > 0:
                print(f'OFFSET {event.offset} HAS ALREADY '
                      'BEEN PROCESSED. CLOSING AND MOVING ON.')

                item_id = data['content'][0]['uuid']

                kv_status = event.body.data.get('status')
                issue = None
                if kv_status == 'skipped':
                    issue = {"issue_type": "NOT_ISSUE"}
                self.rp_service.finish_test_item(item_id=item_id,
                                                 end_time=timestamp(),
                                                 status=kv_status,
                                                 issue=issue)
                return

        kv_name = event.body.data.get('nodeid')
        kv_status = event.body.data.get('status')
        # TODO: change domain in pytest-kafkavents to nodespace
        nodespace = event.body.data.get('domain')
        nodelist = nodespace.split('.')
        name = nodelist[-1:][0]
        print(f'NAME: {name}')
        self.create_missing_testnodes(nodespace)

        print('Creating a test item entry')
        parent_id = \
            self.get_parent_id(nodespace)
        # FIXME: add user provided attr key:values
        item_id = \
            self.rp_service.start_test_item(parent_item_id=parent_id,
                                            name=name,
                                            description=kv_name,
                                            start_time=timestamp(),
                                            item_type="TEST",
                                            attributes={
                                                "kv_offset": event.offset,
                                                "kv_session": self.sessionid})
        print(f'RP ITEM ID: {item_id}')
        # FIXME: "split-brain" happens when interrupted here
        #           so a new parent is created???
        issue = None
        print(f'TEST STATUS: {kv_status}')
        if kv_status == 'skipped':
            issue = {"issue_type": "NOT_ISSUE"}
        self.rp_service.finish_test_item(item_id=item_id,
                                         end_time=timestamp(),
                                         status=kv_status,
                                         issue=issue)

        if self.in_progress:
            self.offset_last = event.offset
