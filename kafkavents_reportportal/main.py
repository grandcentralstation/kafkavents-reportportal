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
import sys
from time import time

from confluent_kafka import Consumer, TopicPartition
from reportportal_client import ReportPortalService
import urllib3

from kafkavents_reportportal.rpbridge import RPBridge


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def timestamp():
    """Timestamp helper func."""
    return str(int(time() * 1000))


class KafkaventsReportPortal():
    """The bridge between Kafka and ReportPortal."""

    def __init__(self, kafka_conf=None, rp_conf=None):
        """Initialize the bridge."""
        self.session_in_progress = False
        self.datastore = os.getenv('KV_DATASTORE', '/datastore')
        if not os.path.exists(self.datastore):
            print(f'Creating datastore directory: {self.datastore}')
            os.makedirs(self.datastore)
        print(f'USING {self.datastore} for datastore')

        self.topic = os.getenv('KV_TOPIC', 'kafkavents')

        self.setup_kafka()
        self.setup_reportportal()

        self.launch = None
        self.node_paths = {}
        self.kv = {}
        self.kv['node_paths'] = {}
        # TODO: figure out which of the above can go

    def setup_kafka(self):
        """Configure the Kafka connection."""
        # Setup Kafka
        kafka_file = None
        for file_path in ['/run/secrets/kafka_secret',
                          '/usr/local/etc/kafkavents/kafka.json']:
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

            # SETUP KV
            kafkaconf['client.id'] = 'kafkavents-reportportal'
            self.kv_host = kafkaconf['bootstrap.servers']
            self.kafkacons = Consumer(kafkaconf)
            self.kafkacons.subscribe([self.topic])
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
                          '/usr/local/etc/kafkavents/rp_conf.json']:
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
        self.rp_host = os.getenv('RP_HOST', rpconf.get('RP_HOST'))
        self.rp_project = os.getenv('RP_PROJECT', rpconf.get('RP_PROJECT'))
        self.rp_token = os.getenv('RP_TOKEN', rpconf.get('RP_TOKEN'))

        if self.rp_host is not None:
            self.service = ReportPortalService(endpoint=self.rp_host,
                                               project=self.rp_project,
                                               token=self.rp_token)
            self.service.session.verify = False

            print(f'Conversing with ReportPortal {self.rp_host} '
                  f'on project {self.rp_project}')
        else:
            print('ERROR: No ReportPortal config provided')
            # TODO: refactor with most graceful exit method
            sys.exit(1)

    @staticmethod
    def get_packet_header(packet):
        """Get the Kafkavents header from the packet."""
        # TODO: add ability to get the actual kafka header too
        # TODO: stabilize packet DSL
        # TODO: add validation for packet schema
        return packet.get('header', None)

    @staticmethod
    def get_packet_event(packet):
        """Get the Kafkavents event section from the packet."""
        return packet.get('event', None)

    def get_parent_id(self, node_path):
        """Get the parent id for the nodepath."""
        nodelist = node_path.split('.')

        if len(nodelist) == 1:
            #print('using launch id')
            parent_id = self.bridge.launch
        else:
            #print('looking up id')
            parentlist = nodelist[:-1]
            parent_domainpath = '.'.join(parentlist)
            parent_id = self.node_paths[parent_domainpath]
        #print(f'Domain {node_path}, Parent {parent_id}')
        return parent_id

    def create_missing_testnodes(self, nodeid):
        """Create a list of node paths from a test nodeid."""
        # TODO: would test namespace be better here than domain path?
        nodetets = nodeid.split('.')
        counter = 0
        while counter < len(nodetets) - 1:
            range_end = counter - len(nodetets) + 1
            #print(f'COUNTER: {counter}:{range_end}')
            node_path = ".".join(nodetets[0:range_end])
            if node_path not in self.node_paths:
                parent_id = self.get_parent_id(node_path)
                #print(f'CREATING NODE {node_path} with parent {parent_id}')
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

                # a bit of a misnomer hack here.
                # uses a setter to set an entry in the dictionary
                # not the entire dictionary
                self.bridge.testnodes = {node_path: suite_id}
            counter = counter + 1

    def listen(self):
        """Listen for Kafka messages."""
        # Listen from the end (or thereabouts)
        topicpart = TopicPartition('kafkavents', 0)
        _, self.listen_offset = self.kafkacons.get_watermark_offsets(topicpart)
        print(f'READ OFFSET: {self.listen_offset}')
        if os.getenv('KAFKA_OFFSET', None) is not None:
            self.listen_offset = int(os.getenv('KAFKA_OFFSET'))

        resume_sessionid = os.getenv('KV_RESUME', False)
        if resume_sessionid:
            print ('RESUMING SESSION')
            self.bridge = RPBridge(resume_sessionid, datastore=self.datastore,
                                   topic=self.topic, restore=True)
            self.listen_offset = self.bridge.offset_last
            self.session_in_progress = True
            self.service.launch_id = self.bridge.launch

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
                    event_data = json.loads(kevent.value())
                    kv_header = self.get_packet_header(event_data)
                    sessionid = kv_header.get('session_id', None)
                    packet_num = kv_header.get('packetnum', None)
                    kv_type = kv_header.get('type', None)
                    kv_event = self.get_packet_event(event_data)
                    print(f"kafkavent session {sessionid} "
                          f"packet # {packet_num}: type {kv_type}")
                    print(kv_event)

                    if kv_type == "sessionstart":
                        print('SESSION START')
                        self.node_paths = {}
                        #self.bridge.offset_start = message_offset
                        # Start a KV Bridge Session
                        self.bridge = RPBridge(sessionid,
                                               topic=self.topic,
                                               offset_start=message_offset,
                                               datastore=self.datastore)
                        launch_name = kv_event.get('name')
                        print(f"Starting launch: {launch_name}")
                        # Start launch
                        attributes = [{'key': 'sessiondid',
                                       'value': sessionid}]
                        description = 'Created by the bridge'
                        self.bridge.launch = \
                            self.service.start_launch(name=launch_name,
                                                      start_time=timestamp(),
                                                      description=description,
                                                      attributes=attributes)
                        self.kv['launch'] = self.bridge.launch
                        # TODO: configurize description ^^^
                        print('LAUNCH: {}', self.bridge.launch)
                        self.session_in_progress = True
                        self.bridge.offset_last = message_offset

                    if kv_type == "sessionend":
                        print('SESSION END')
                        if not self.session_in_progress:
                            #TODO: handle mid-session restarts
                            continue
                        self.kv['offset_end'] = message_offset
                        self.bridge.offset_end = message_offset

                        launch_name = kv_event.get('name')
                        print(f"Ending launch: {launch_name}")
                        # Finish launch.
                        self.service.finish_launch(launch=self.bridge.launch,
                                                   end_time=timestamp())
                        print(self.node_paths)

                        #os.unlink(f'{self.datastore}/session.current')
                        if self.session_in_progress:
                            self.bridge.offset_last = message_offset
                            self.session_in_progress = False
                            self.bridge.end()

                    if kv_type == "testresult":
                        # session interrupted? read from cache
                        if not self.session_in_progress:
                            # TODO: refactor this to the bridge.resume()
                            print('WARNING: NO SESSION IN PROGRESS. '
                                  'Skipping to the next SESSION END')
                            '''
                            cachefile = f'datastore/{sessionid}.datastore'
                            if os.path.exists(cachefile):
                                with open(cachefile) as fh:
                                    self.kv = json.load(fh)
                                print(self.kv)
                                self.node_paths = self.kv['testnodes']
                                self.session_in_progress = True
                                print(self.node_paths)
                                self.service.launch_id = self.kv['launch']
                                self.bridge = RPBridge(sessionid,
                                                       topic=self.topic,
                                                       datastore=self.datastore)

                            else:
                                continue
                            '''
                            continue
                        kv_name = kv_event.get('nodeid')
                        kv_status = kv_event.get('status')
                        # TODO: change domain in pytest-kafkavents to nodespace
                        nodespace = kv_event.get('domain')
                        nodelist = nodespace.split('.')
                        name = nodelist[-1:][0]
                        print(f'NAME: {name}')
                        self.create_missing_testnodes(nodespace)

                        print('Creating a test item entry')
                        parent_id = self.get_parent_id(nodespace)
                        # FIXME: replace sample attr key:values
                        item_id = \
                            self.service.start_test_item(
                                parent_item_id=parent_id,
                                name=name,
                                description=kv_name,
                                start_time=timestamp(),
                                item_type="TEST",
                                attributes={"key1": "val1",
                                            "key2": "val2"})
                        print(f'RP ITEM ID: {item_id}')
                        self.service.finish_test_item(item_id=item_id,
                                                      end_time=timestamp(),
                                                      status=kv_status)

                        if self.session_in_progress:
                            self.bridge.offset_last = message_offset

                    if kv_type == "summary":
                        print("SUMMARY")
                        #self.bridge.offset_last = message_offset

                    # we made it this far, write the offset for this event
                    #if self.session_in_progress:
                    #    self.bridge.offset_last = message_offset

                    # write the session cache
                    self.kv['node_paths'] = self.node_paths
                    #self.bridge.testnodes = self.node_paths
                    '''
                    with open(f'{self.datastore}/{sessionid}.cache', "w") as ch:
                        json.dump(self.kv, ch, indent=2, sort_keys=True)
                    '''
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
        KV_OFFSET
        KV_RESUME
        KV_DATASTORE
        KV_TOPIC
    """
    kafka = KafkaventsReportPortal()
    kafka.listen()


if __name__ == '__main__':
    main()

# TODO: LINT!!!!
# TODO: listen to multiple topics
# TODO: more importantly, track multiple sessions
