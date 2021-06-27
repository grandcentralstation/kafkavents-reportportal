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
from time import time

from confluent_kafka import Consumer, TopicPartition
from reportportal_client import ReportPortalService
import urllib3

from kafkavents.rpbridge import RPBridge


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def timestamp():
    """Timestamp helper func."""
    return str(int(time() * 1000))


class KafkaventsReportPortal(object):
    """The bridge between Kafka and ReportPortal."""

    def __init__(self):
        """Initialize the bridge.

        Required Env Vars:
        RP_HOST
        RP_PROJECT
        RP_TOKEN

        KAFKA_CONF
        KAFKA_OFFSET
        KV_DATASTORE
        KV_TOPIC
        """
        self.session_in_progress = False
        self.datastore = os.getenv('KV_DATASTORE', '/datastore')
        if not os.path.exists:
            print(f'Creating datastore directory: {self.datastore}')
            os.makedirs(self.datastore)
        self.topic = os.getenv('KV_TOPIC', 'kafkavents')

        self.setup_kafka()
        self.setup_reportportal()

    def setup_kafka(self):
        """Configure the Kafka connection."""
        # Setup Kafka
        kafka_file = os.getenv('KAFKA_CONF',
                               '/usr/local/etc/kafkavents/kafka.json')
        fileh = open(kafka_file)
        kafkaconf = json.load(fileh)
        fileh.close()

        # SETUP KV
        kafkaconf['client.id'] = 'kafkavents-reportportal'
        self.kv_host = kafkaconf['bootstrap.servers']
        self.kafkacons = Consumer(kafkaconf)
        self.kafkacons.subscribe([self.topic])
        _, self.listen_offset = self.kafkacons.get_watermark_offsets(TopicPartition('kafkavents', 0))
        #print(f'READ OFFSET: {self.listen_offset}')
        if os.getenv('KAFKA_OFFSET', None) is not None:
            self.listen_offset = int(os.getenv('KAFKA_OFFSET'))
        self.kafkacons.assign([TopicPartition('kafkavents', partition=0, offset=self.listen_offset)])
        self.kafkacons.commit()
        print(f'Conversing with Kafka {self.kv_host} on topic {self.topic}')

    def setup_reportportal(self):
        """Configure the ReportPortal connection."""
        # setup reportportal
        self.rp_host = os.getenv('RP_HOST', None)
        self.rp_project = os.getenv('RP_PROJECT', None)
        self.rp_token = os.getenv('RP_TOKEN', None)

        self.service = ReportPortalService(endpoint=self.rp_host,
                                           project=self.rp_project,
                                           token=self.rp_token)
        self.service.session.verify = False
        self.launch = None
        self.node_paths = {}
        self.kv = {}
        self.kv['node_paths'] = {}
        #self.kv['launch'] = None
        print(f'Conversing with ReportPortal {self.rp_host} '
              f'on project {self.rp_project}')

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
                self.bridge.testnodes[node_path] = suite_id
                #print(f'CREATED NODE {node_path} with ID {suite_id} and parent {parent_id}')
            counter = counter + 1

    def listen(self):
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
                    # Something happened. Check event.
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
                        self.bridge.launch = self.service.start_launch(name=launch_name,
                                                                      start_time=timestamp(),
                                                                      description='Created by the bridge',
                                                                      attributes=[{'key': 'sessiondid', 'value': sessionid}])
                        self.kv['launch'] = self.bridge.launch
                        # TODO: configurize description ^^^
                        print('LAUNCH: {}', self.bridge.launch)
                        #self.node_paths[sessionid] = self.bridge.launch
                        self.session_in_progress = True

                        f = open(f"{self.datastore}/session.current", "w")
                        f.write(self.bridge.launch)
                        f.close()

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

                        os.unlink(f'{self.datastore}/session.current')
                        self.session_in_progress = False

                    if kv_type == "testresult":
                        # session interrupted? read from cache
                        if not self.session_in_progress:
                            # TODO: refactor this to the bridge.resume()
                            cachefile = f'datastore/{sessionid}.cache'
                            if os.path.exists(cachefile):
                                with open(cachefile) as fh:
                                    self.kv = json.load(fh)
                                print(self.kv)
                                self.node_paths = self.kv['node_paths']
                                self.session_in_progress = True
                                print(self.node_paths)
                                self.service.launch_id = self.bridge.launch
                            else:
                                continue

                        kv_name = kv_event.get('nodeid')
                        kv_status = kv_event.get('status')
                        # TODO: change domain in pytest-kafkavents to nodespace
                        nodespace = kv_event.get('domain')
                        nodelist = nodespace.split('.')
                        name = nodelist[-1:][0]
                        print(f'NAME: {name}')
                        self.create_missing_testnodes(nodespace)

                        print(f'Creating a test item entry')
                        parent_id = self.get_parent_id(nodespace)
                        # FIXME: replace sample attr key:values
                        item_id = self.service.start_test_item(parent_item_id=parent_id,
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

                    # we made it this far, write the offset for this event
                    with open(f"{self.datastore}/offset", "w") as fileh:
                        fileh.write(f'{message_offset}')
                    if self.session_in_progress:
                        self.bridge.offset_last = message_offset

                    # write the session cache
                    self.kv['node_paths'] = self.node_paths
                    #self.bridge.testnodes = self.node_paths
                    with open(f'{self.datastore}/{sessionid}.cache', "w") as ch:
                        json.dump(self.kv, ch, indent=2, sort_keys=True)

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.kafkacons.close()


# FIXME: add main test here

kafka = KafkaventsReportPortal()
kafka.listen()

# TODO: LINT!!!!
# TODO: listen to multiple topics
# TODO: more importantly, track multiple sessions
