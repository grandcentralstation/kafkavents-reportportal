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

from confluent_kafka import Consumer
from reportportal_client import ReportPortalService
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def timestamp():
    return str(int(time() * 1000))


class KafkaventsReportPortalBridge(object):
    def __init__(self):
        # Setup Kafka
        #use OpenShift secrets to put this in place then point to it here
        kafka_file = os.getenv('KAFKA_CONF',
                               '/usr/local/etc/kafkavents/kafka.json')
        fileh = open(kafka_file)
        kafkaconf = json.load(fileh)
        fileh.close()

        kafkaconf['group.id'] = 'kafkavents_reportportal'
        self.kafkacons = Consumer(kafkaconf)
        self.kafkacons.subscribe(['kafkavents'])

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

    @staticmethod
    def get_packet_header(packet):
        return packet.get('header', None)

    @staticmethod
    def get_packet_event(packet):
        return packet.get('event', None)

    def get_parent_id(self, domain_path):
        domainlist = domain_path.split('.')

        if len(domainlist) == 1:
            #print('using launch id')
            parent_id = self.launch
        else:
            #print('looking up id')
            parentlist = domainlist[:-1]
            parent_domainpath = '.'.join(parentlist)
            parent_id = self.domain_paths[parent_domainpath]
        #print(f'Domain {domain_path}, Parent {parent_id}')
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
        print('Listening...')
        try:
            while True:
                kevent = self.kafkacons.poll(1.0)
                if kevent is None:
                    # nothing to see here. just waiting for a message.
                    continue
                elif kevent.error():
                    print('error: {}'.format(kevent.error()))
                else:
                    # Something happened. Check event.
                    event_data = json.loads(kevent.value())
                    kv_header = self.get_packet_header(event_data)
                    sessionid = kv_header.get('session_id', None)
                    packet_num = kv_header.get('packetnum', None)
                    kv_type = kv_header.get('type', None)
                    kv_event = self.get_packet_event(event_data)
                    print(f"kafkavent session {sessionid} "
                          "packet # {packet_num}: type {kv_type}")
                    print(kv_event)

                    if kv_type == "sessionstart":
                        self.domain_paths = {}
                        launch_name = kv_event.get('name')
                        print(f"Starting launch: {launch_name}")
                        # Start launch.
                        self.launch = self.service.start_launch(name=launch_name,
                                                      start_time=timestamp(),
                                                      description="My test launch")
                        #print(f'LAUNCH: {self.launch}')
                        self.domain_paths[sessionid] = self.launch

                    if kv_type == "sessionend":
                        launch_name = kv_event.get('name')
                        print(f"Ending launch: {launch_name}")
                        # Finish launch.
                        self.service.finish_launch(launch=self.launch,
                                                   end_time=timestamp())
                        print(self.domain_paths)

                    if kv_type == "testresult":
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
                        # Finish test item Report Portal versions below 5.0.0.
                        self.service.finish_test_item(item_id=item_id,
                                                      end_time=timestamp(),
                                                      status=kv_status)
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.kafkacons.close()


kafka = KafkaventsReportPortalBridge()
kafka.listen()
