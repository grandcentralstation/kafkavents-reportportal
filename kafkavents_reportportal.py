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
import json
import os
from time import time

from confluent_kafka import Consumer, TopicPartition
from reportportal_client import ReportPortalService
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def timestamp():
    return str(int(time() * 1000))


class KafkaventsReportPortalBridge(object):
    def __init__(self):
        self.session_in_progress = False

        # Setup Kafka
        #use OpenShift secrets to put this in place then point to it here
        kafka_file = os.getenv('KAFKA_CONF',
                               '/usr/local/etc/kafkavents/kafka.json')
        fileh = open(kafka_file)
        kafkaconf = json.load(fileh)
        fileh.close()

        # SETUP KV
        self.topic = os.getenv('KV_TOPIC', 'kafkavents')

        kafkaconf['client.id'] = 'kafkavents-reportportal'
        #kafkaconf['enable.auto.commit'] = "true"
        self.kafkacons = Consumer(kafkaconf)
        self.kafkacons.subscribe([self.topic])
        #TODO: unwire kafkavents topic
        _, self.end_offset = self.kafkacons.get_watermark_offsets(TopicPartition('kafkavents', 0))
        print(f'READ OFFSET: {self.end_offset}')
        if os.getenv('KAFKA_OFFSET', None) is not None:
            self.end_offset = int(os.getenv('KAFKA_OFFSET'))
        self.kafkacons.assign([TopicPartition('kafkavents', partition=0, offset=self.end_offset)])
        self.kafkacons.commit()



        # setup reportportal
        self.rp_host = os.getenv('RP_HOST', None)
        self.rp_project = os.getenv('RP_PROJECT', None)
        self.rp_token = os.getenv('RP_TOKEN', None)

        self.service = ReportPortalService(endpoint=self.rp_host,
                                            project=self.rp_project,
                                            token=self.rp_token)
        self.service.session.verify = False
        self.launch = None
        self.domain_paths = {}
        self.kv = {}
        self.kv['domain_paths'] = {}
        self.kv['launch'] = None

    @staticmethod
    def get_packet_header(packet):
        return packet.get('header', None)

    @staticmethod
    def get_packet_event(packet):
        return packet.get('event', None)

    def get_parent_id(self, domain_path):
        domainlist = domain_path.split('.')

        if len(domainlist) == 1:
            print('using launch id')
            parent_id = self.kv['launch']
        else:
            print('looking up id')
            parentlist = domainlist[:-1]
            parent_domainpath = '.'.join(parentlist)
            parent_id = self.domain_paths[parent_domainpath]
        print(f'Domain {domain_path}, Parent {parent_id}')
        return parent_id

    def create_missing_domainpaths(self, domain):
        domaintets = domain.split('.')
        counter = 0
        while counter < len(domaintets) - 1:
            offset = counter - len(domaintets) + 1
            #print(f'COUNTER: {counter}:{offset}')
            domain_path = ".".join(domaintets[0:offset])
            if not domain_path in self.domain_paths:
                parent_id = self.get_parent_id(domain_path)
                #print(f'CREATING DOMAIN {domain_path} with parent {parent_id}')
                if counter == 0:
                    # RP requires parent_uuid when parent is launch
                    suite_id = self.service.start_test_item(parent_uuid=parent_id,
                                                            name=domaintets[counter],
                                                            start_time=timestamp(),
                                                            item_type="SUITE")
                else:
                    suite_id = self.service.start_test_item(parent_item_id=parent_id,
                                                            name=domaintets[counter],
                                                            start_time=timestamp(),
                                                            item_type="SUITE")
                self.domain_paths[domain_path] = suite_id
                #print(f'CREATED DOMAIN {domain_path} with ID {suite_id} and parent {parent_id}')
            counter = counter + 1

    def listen(self):
        print(f'Listening at offset {self.end_offset} ...')
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
                        #TODO: mark start of session to prevent mid-sess fails
                        print('SESSION START')
                        self.domain_paths = {}
                        self.kv['offset_start'] = message_offset

                        launch_name = kv_event.get('name')
                        print(f"Starting launch: {launch_name}")
                        # Start launch.
                        self.kv['launch'] = self.service.start_launch(name=launch_name,
                                                      start_time=timestamp(),
                                                      description=sessionid)
                        # TODO: configurize description ^^^
                        #print(f'LAUNCH: {self.launch}')
                        self.domain_paths[sessionid] = self.kv['launch']
                        self.session_in_progress = True

                        f = open(f"/datastore/session.current", "w")
                        f.write(self.kv['launch'])
                        f.close()

                    if kv_type == "sessionend":
                        print('SESSION END')
                        if not self.session_in_progress:
                            #TODO: handle mid-session restarts
                            continue
                        self.kv['offset_end'] = message_offset

                        launch_name = kv_event.get('name')
                        print(f"Ending launch: {launch_name}")
                        # Finish launch.
                        self.service.finish_launch(launch=self.kv['launch'],
                                                   end_time=timestamp())
                        print(self.domain_paths)

                        os.unlink("/datastore/session.current")
                        self.session_in_progress = False

                    if kv_type == "testresult":
                        # session interrupted? read from cache
                        if not self.session_in_progress:
                            cachefile = f'datastore/{sessionid}.cache'
                            if os.path.exists(cachefile):
                                with open(cachefile) as fh:
                                    self.kv = json.load(fh)
                                print(self.kv)
                                self.domain_paths = self.kv['domain_paths']
                                self.session_in_progress = True
                                print(self.domain_paths)
                                self.service.launch_id = self.kv['launch']
                            else:
                                continue

                        kv_name = kv_event.get('nodeid')
                        kv_status = kv_event.get('status')
                        domain = kv_event.get('domain')
                        domainlist = domain.split('.')
                        name = domainlist[-1:][0]
                        print(f'NAME: {name}')
                        self.create_missing_domainpaths(domain)

                        print(f'Creating a test item entry')
                        parent_id = self.get_parent_id(domain)
                        item_id = self.service.start_test_item(parent_item_id=parent_id,
                                                               name=name,
                                                               description=kv_name,
                                                               start_time=timestamp(),
                                                               item_type="TEST",
                                                               parameters={"key1": "val1",
                                                                           "key2": "val2"})
                        print(f'ITEM: {item_id}')
                        self.service.finish_test_item(item_id=item_id,
                                                      end_time=timestamp(),
                                                      status=kv_status)

                    # we made it this far, write the offset for this event
                    with open("/datastore/offset", "w") as fileh:
                        fileh.write(f'{message_offset}')

                    # write the session cache
                    self.kv['domain_paths'] = self.domain_paths
                    with open(f'/datastore/{sessionid}.cache', "w") as ch:
                        json.dump(self.kv, ch, indent=2, sort_keys=True)

        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.kafkacons.close()


kafka = KafkaventsReportPortalBridge()
kafka.listen()
