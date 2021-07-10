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
"""This module serves as a real-time bridge between Kafka and ReportPortal."""
import json
import os
from requests.exceptions import ConnectionError, HTTPError
import sys
import time

from confluent_kafka import TopicPartition
import urllib3

from kafkavents_reportportal.session import ReportPortalSession
from kafkavents_reportportal.kafka import Kafka
from kafkavents_reportportal.reportportal import ReportPortal
from kafkavents_reportportal.event import Event


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def timestamp():
    """Timestamp helper func."""
    return str(int(time.time() * 1000))


class KafkaventsReportPortal():
    """The bridge between Kafka and ReportPortal."""

    def __init__(self, kafka_conf=None, rp_conf=None):
        """Initialize the bridge."""
        # FIXME: kafka_conf and rp_conf aren't passed in here
        self.session_in_progress = False
        self.sessions = {}
        self.datastore = os.getenv('KV_DATASTORE', '/datastore')
        if not os.path.exists(self.datastore):
            print(f'Creating datastore directory: {self.datastore}')
            os.makedirs(self.datastore)
        print(f'DATASTORE: {self.datastore}')

        self.topic = os.getenv('KV_TOPIC', 'kafkavents')

        self.autorecover = os.getenv('KV_AUTORECOVER', False)

        self.replay = False
        if os.getenv('KV_REPLAY', False):
            self.replay = True
            self.replay_sessionid = os.getenv('KV_REPLAY')

        self.setup_kafka()
        self.setup_reportportal()

        self.launch = None
        self.node_paths = {}
        self.suite_stack = []
        self.kv = {}
        self.kv['node_paths'] = {}
        # TODO: figure out which of the above can go

    def setup_kafka(self):
        """Configure the Kafka connection."""
        # Setup Kafka
        kafka_file = None
        for file_path in ['/run/secrets/kafka_secret',
                          '/usr/local/kafkavents/kafka.json']:
            print(f'CHECKING {file_path}')
            if os.path.exists(file_path):
                kafka_file = file_path
                break
        kafka_file = os.getenv('KAFKA_CONF', kafka_file)

        if kafka_file is not None:
            print(f'READING {kafka_file}')
            fileh = open(kafka_file)
            kafkaconf = json.load(fileh)
            fileh.close()

            kafka = Kafka(kafkaconf)
            kafka.topic = self.topic
            kafka.client_id = 'kafkavents-reportportal'

            if self.replay:
                kafka.group_id = 'kafkavents-replay'

            self.kv_host = kafka.bootstrap_servers
            self.kafkacons = kafka.connect()
        else:
            print('ERROR: No Kafka config provided')
            # TODO: refactor with most graceful exit method
            sys.exit(1)

    def setup_reportportal(self):
        """Configure the ReportPortal connection."""
        # Setup reportportal

        rpconf = {}
        rp_file = None
        for file_path in ['/run/secrets/reportportal_secret',
                          '/usr/local/kafkavents/reportportal.json']:
            print(f'CHECKING {file_path}')
            if os.path.exists(file_path):
                rp_file = file_path
                break
        rp_file = os.getenv('RP_CONF', rp_file)

        if rp_file is not None:
            print(f'READING {rp_file}')
            fileh = open(rp_file)
            rpconf = json.load(fileh)
            fileh.close()

            # Override config with ENV var
            rpconf['RP_HOST'] = os.getenv('RP_HOST',
                                          rpconf.get('RP_HOST'))
            rpconf['RP_PROJECT'] = os.getenv('RP_PROJECT',
                                             rpconf.get('RP_PROJECT'))
            rpconf['RP_TOKEN'] = os.getenv('RP_TOKEN',
                                           rpconf.get('RP_TOKEN'))

            self.reportportal = ReportPortal(rpconf)
            self.service = self.reportportal.service
        else:
            print('ERROR: No ReportPortal config provided')
            # TODO: refactor with most graceful exit method
            sys.exit(1)

    # TODO: add ability to get the actual kafka header too
    # TODO: stabilize packet DSL
    # TODO: add validation for packet schema


    @staticmethod
    def get_packet_event(packet):
        """Get the Kafkavents event section from the packet."""
        return packet.get('event', None)

    def get_parent_id(self, node_path, session):
        """Get the parent id for the nodepath."""
        nodetets = node_path.split('.')
        nodetets.pop()
        if len(nodetets) == 0:
            print('using launch id')
            parent_id = session.launch
        else:
            #print('looking up id')
            parent_id = session.testnodes.get('.'.join(nodetets))
        #print(f'Domain {node_path}, Parent {parent_id}')
        return parent_id

    def create_missing_testnodes(self, nodeid, session=None):
        """Create a list of node paths from a test nodeid."""
        print(f'CREATE MISSING TESTNODES.... SESSION {session}')
        nodetets = nodeid.split('.')
        counter = 0
        while counter < len(nodetets) - 1:
            range_end = counter - len(nodetets) + 1
            #print(f'COUNTER: {counter}:{range_end}')
            node_path = ".".join(nodetets[0:range_end])

            if node_path not in session.testnodes:
                parent_id = self.get_parent_id(node_path, session)
                print(f'CREATING NODE {node_path} with parent {parent_id}')
                if counter == 0:
                    # RP requires parent_uuid when parent is launch
                    suite_id = \
                        self.service.start_test_item(parent_uuid=parent_id,
                                                     name=nodetets[counter],
                                                     start_time=timestamp(),
                                                     item_type="SUITE")
                else:
                    suite_id = \
                        self.service.start_test_item(parent_item_id=parent_id,
                                                     name=nodetets[counter],
                                                     start_time=timestamp(),
                                                     item_type="SUITE")
                self.node_paths[node_path] = suite_id
                self.suite_stack.append(suite_id)

                # a bit of a misnomer hack here.
                # uses a setter to set an entry in the dictionary
                # not the entire dictionary
                # FIXME: this should work with a direct assign
                #session.testnodes = {node_path: suite_id}
                #print(f'CREATE NODES: {session.testnodes}')
                if session.testnodes is not None:
                    session.testnodes = {node_path: suite_id}
            counter = counter + 1

    def recover_session(self):
        """Auto-recover or resume a specific session."""
        resume_sessionid = None

        # FIXME: we do this check twice. assume we're here for a reason
        print('AUTORECOVER mode is on')
        # check for incomplete session
        resume_sessionid = \
            ReportPortalSession.recover(self.datastore)

        resume_sessionid = os.getenv('KV_RESUME_SESSION', resume_sessionid)
        if resume_sessionid is not None:
            print(f'RESUMING SESSION: {resume_sessionid}')
            self.sessions[resume_sessionid] = \
                ReportPortalSession(resume_sessionid,
                                    datastore=self.datastore,
                                    topic=self.topic, restore=True)
            last_offset = self.sessions[resume_sessionid].offset_last
            if last_offset is not None:
                self.listen_offset = \
                    self.sessions[resume_sessionid].offset_last + 1
                self.session_in_progress = True
                self.service.launch_id = self.sessions[resume_sessionid].launch
            else:
                return None

        return resume_sessionid

    def listen(self):
        """Listen for Kafka messages."""
        # Listen from the end (or thereabouts)
        topicpart = TopicPartition('kafkavents', 0)
        _, self.listen_offset = self.kafkacons.get_watermark_offsets(topicpart)
        print(f'READ OFFSET: {self.listen_offset}')
        if os.getenv('KAFKA_OFFSET', None) is not None:
            self.listen_offset = int(os.getenv('KAFKA_OFFSET'))

        rerun = False
        rerun_launchid = None
        if self.replay:
            self.replay_start, self.replay_end, rerun_launchid = \
                ReportPortalSession.read_offsets(self.replay_sessionid,
                                                 self.datastore)
            self.listen_offset = self.replay_start
            #rerun = True
            print(f'REPLAY: start {self.replay_start} end {self.replay_end}')

        recovering_session_flag = False
        if self.autorecover:
            recovering_session = self.recover_session()
            if recovering_session:
                recovering_session_flag = True

        self.kafkacons.assign([TopicPartition('kafkavents', partition=0,
                                              offset=self.listen_offset)])
        self.kafkacons.commit()
        print(f'Conversing with Kafka {self.kv_host} on topic {self.topic}')

        print(f'Listening at offset {self.listen_offset} ...')
        try:
            while True:
                kevent = self.kafkacons.poll(1.0)
                if kevent is None:
                    # nothing to see here. just waiting for a message.
                    continue
                elif kevent.error():
                    print('error: {}'.format(kevent.error()))
                else:
                    topic = kevent.topic()
                    message_offset = kevent.offset()
                    partition = kevent.partition()
                    print(f'\nTOPIC: {topic} PARTITION: {partition} '
                          f'OFFSET: {message_offset}')
                    self.kv['offset'] = message_offset
                    # Something occurred. Check event.
                    event = Event(kevent)
                    event.offset = message_offset
                    packet = event.packet
                    print(f'PACKET: {packet}')

                    sessionid = event.sessionid
                    print(f'SESSIONID: {event.sessionid}')
                    if self.replay:
                        sessionid += '-replay'

                    if sessionid is not None:
                        if self.sessions.get(sessionid, None) is None:
                            self.sessions[sessionid] = \
                                ReportPortalSession(sessionid,
                                                    topic=self.topic,
                                                    offset_start=event.offset,
                                                    datastore=self.datastore)

                    kv_event = event.body.data

                    self.sessions[sessionid].handle_event(event)

                    if event.event_type == "sessionstart":
                        print('SESSION START')
                        #self.sessions[sessionid].start(event)
                        self.node_paths = {}
                        self.suite_stack = []
                        #self.bridge.offset_start = message_offset
                        # Start a KV Bridge Session
                        # TODO: remove when the session class handles things
                        #self.sessions[sessionid].bridge = self.bridge

                        launch_name = kv_event.get('name')
                        print(f"Starting launch: {launch_name}")
                        # Start launch
                        attributes = [{'key': 'sessionid',
                                       'value': sessionid}]
                        description = 'Created by the bridge'
                        success = False
                        while not success:
                            try:
                                launch = \
                                    self.service.start_launch(name=launch_name,
                                                              start_time=timestamp(),
                                                              description=description,
                                                              rerun=False,
                                                              rerunOf=None,
                                                              attributes=attributes)
                                #self.bridge.launch = launch
                                self.sessions[sessionid].launch = launch
                                success = True
                            except HTTPError as err:
                                print(f'ERROR: {err} '
                                      '\nRetrying in 300 seconds')
                                time.sleep(300)
                            except ConnectionError as err:
                                print(f'ERROR: {err}'
                                      '\nRetrying in 300 seconds')
                                time.sleep(300)

                        #self.kv['launch'] = self.bridge.launch
                        # TODO: configurize description ^^^
                        print('LAUNCH: {}', self.sessions[sessionid].launch)
                        self.session_in_progress = True
                        #self.bridge.start()
                        #self.bridge.offset_last = message_offset

                    if event.event_type == "sessionend":
                        print('SESSION END')
                        if not self.session_in_progress:
                            # TODO: handle mid-session restarts
                            continue
                        self.kv['offset_end'] = message_offset
                        self.sessions[sessionid].offset_end = message_offset

                        launch_name = kv_event.get('name')
                        print(f"Ending launch: {launch_name}")
                        # Finish launch.
                        self.service.finish_launch(launch=self.sessions[sessionid].launch,
                                                   end_time=timestamp())
                        print(self.node_paths)

                        self.sessions[sessionid].offset_last = message_offset
                        # Close the suites
                        for itemid in reversed(self.suite_stack):
                            self.service.finish_test_item(item_id=itemid,
                                                          end_time=timestamp(),
                                                          status=None)
                            #print(f'CLOSED: {itemid}')
                        self.session_in_progress = False
                        #self.bridge.end()
                        self.sessions[sessionid].end_event(event)

                    if event.event_type == "testresult":
                        # session interrupted? read from cache
                        if not self.session_in_progress:
                            # TODO: refactor this to the bridge.resume()
                            print('WARNING: NO SESSION IN PROGRESS. '
                                  'Skipping to the next SESSION END')
                            continue

                        # check to see if we're about to dupe a testitem
                        if recovering_session_flag:
                            # Get the int id for the launch
                            url = (f'launch?'
                                   f'filter.eq.uuid={self.sessions[sessionid].launch}')
                            data = self.reportportal.api_get(url)
                            data = json.loads(data.text)
                            launch_num = data['content'][0]['id']

                            # check for an existing test item for offset
                            url = (f'item?filter.has.attributeKey=kv_offset&'
                                   f'filter.has.attributeValue={message_offset}&'
                                   f'filter.eq.launchId={launch_num}')
                            data = self.reportportal.api_get(url)
                            data = json.loads(data.text)
                            print(f'LAUNCH {launch_num} TESTITEM DATA: {data}')
                            num_found = data['page']['totalElements']
                            print(f'NUM_FOUND: {num_found}')

                            # clear the flag and continue if about to dupe
                            recovering_session_flag = False
                            if num_found > 0:
                                print(f'OFFSET {message_offset} HAS ALREADY '
                                      'BEEN PROCESSED. CLOSING AND MOVING ON.')

                                item_id = data['content'][0]['uuid']

                                kv_status = kv_event.get('status')
                                issue = None
                                if kv_status == 'skipped':
                                    issue = {"issue_type": "NOT_ISSUE"}
                                self.service.finish_test_item(item_id=item_id,
                                                              end_time=timestamp(),
                                                              status=kv_status,
                                                              issue=issue)
                                continue

                        kv_name = kv_event.get('nodeid')
                        kv_status = kv_event.get('status')
                        # TODO: change domain in pytest-kafkavents to nodespace
                        nodespace = kv_event.get('domain')
                        nodelist = nodespace.split('.')
                        name = nodelist[-1:][0]
                        print(f'NAME: {name}')
                        self.create_missing_testnodes(nodespace,
                                                      self.sessions[sessionid])

                        print('Creating a test item entry')
                        parent_id = \
                            self.get_parent_id(nodespace,
                                               self.sessions[sessionid])
                        # FIXME: add user provided attr key:values
                        item_id = \
                            self.service.start_test_item(
                                parent_item_id=parent_id,
                                name=name,
                                description=kv_name,
                                start_time=timestamp(),
                                item_type="TEST",
                                attributes={"kv_offset": message_offset,
                                            "kv_session": sessionid})
                        print(f'RP ITEM ID: {item_id}')
                        # FIXME: "split-brain" happens when interrupted here
                        #           so a new parent is created???
                        issue = None
                        print(f'TEST STATUS: {kv_status}')
                        if kv_status == 'skipped':
                            issue = {"issue_type": "NOT_ISSUE"}
                        self.service.finish_test_item(item_id=item_id,
                                                      end_time=timestamp(),
                                                      status=kv_status,
                                                      issue=issue)

                        if self.session_in_progress:
                            self.sessions[sessionid].offset_last = message_offset

                    # TODO: handle misc types here
                    #       better yet, move to event type handler
                    #       so anything below this can be pass thru

                    # this is a cheat to workaround refactoring
                    self.kv['node_paths'] = self.node_paths

                    print(f'SESSIONS: {self.sessions}')

                    if self.replay and message_offset == self.replay_end:
                        print('REPLAY COMPLETE. EXITING.')

                        sys.exit(0)
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.kafkacons.close()


def main():
    """Start this thing up with main.

    Required Env Vars:
        RP_HOST
        RP_PROJECT
        RP_TOKEN

        KAFKA_CONF
        KV_REPLAY
        KV_AUTORECOVER
        KV_OFFSET
        KV_RESUME
        KV_DATASTORE
        KV_TOPIC
    """
    kafka = KafkaventsReportPortal()
    kafka.listen()


if __name__ == '__main__':
    main()

# TODO: listen to multiple topics
# TODO: more importantly, track multiple sessions
# TODO: refactor RP-specifics out of main (see rp_preproc)
# TODO: add if DEBUG to a lot of print statements
