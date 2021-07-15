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
        #self.session_in_progress = False
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

    def setup_kafka(self):
        """Configure the Kafka connection."""
        # Setup Kafka
        kafka_file = None
        # for podman, the secret is mounted at [0]
        # for OpenShift, mount the secret at [1]
        # or specify a dir wih the env variable
        for file_path in ['/run/secrets/kafka_secret',
                          '/usr/local/kafkavents/kafka.json']:
            print(f'CHECKING {file_path}')
            if os.path.exists(file_path):
                kafka_file = file_path
                break
        kafka_file = os.getenv('KAFKA_CONF', kafka_file)

        if kafka_file is not None:
            print(f'READING {kafka_file}')
            with open(kafka_file) as fileh:
                kafkaconf = json.load(fileh)

            kafka = Kafka(kafkaconf)
            kafka.topic = self.topic
            kafka.client_id = 'kafkavents-reportportal'

            # use a different group to avoid messing with the offset
            if self.replay:
                kafka.group_id = 'kafkavents-replay'

            self.bootstrap_servers = kafka.bootstrap_servers
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
        # for podman, the secret is mounted at [0]
        # for OpenShift, mount the secret at [1]
        # or specify a dir wih the env variable
        for file_path in ['/run/secrets/reportportal_secret',
                          '/usr/local/kafkavents/reportportal.json']:
            print(f'CHECKING {file_path}')
            if os.path.exists(file_path):
                rp_file = file_path
                break
        rp_file = os.getenv('RP_CONF', rp_file)

        if rp_file is not None:
            print(f'READING {rp_file}')
            with open(rp_file) as fileh:
                rpconf = json.load(fileh)

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

    # TODO: stabilize packet DSL
    # TODO: add validation for packet schema

    @staticmethod
    def get_packet_event(packet):
        """Get the Kafkavents event section from the packet."""
        return packet.get('event', None)

    def recover_session(self):
        """Auto-recover or resume a specific session."""
        resume_sessionid = None

        # FIXME: we do this check twice. assume we're here for a reason
        print('AUTORECOVER mode is on')
        # check for incomplete session
        resume_sessionid = os.getenv('KV_RESUME_SESSION', None)
        resume_sessionid = \
            ReportPortalSession.recover(self.datastore,
                                        sessionid=resume_sessionid)

        if resume_sessionid is not None:
            print(f'RESUMING SESSION: {resume_sessionid}')
            self.sessions[resume_sessionid] = \
                ReportPortalSession(resume_sessionid,
                                    datastore=self.datastore,
                                    topic=self.topic,
                                    reportportal=self.reportportal,
                                    restore=True)
            last_offset = self.sessions[resume_sessionid].offset_last
            if last_offset is not None:
                self.listen_offset = \
                    self.sessions[resume_sessionid].offset_last
                #self.session_in_progress = True
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
        # TODO: make reruns an option (passed at front-end client)
        if self.replay:
            self.replay_start, self.replay_end, rerun_launchid = \
                ReportPortalSession.read_offsets(self.replay_sessionid,
                                                 self.datastore)
            self.listen_offset = self.replay_start
            #rerun = True
            print(f'REPLAY: start {self.replay_start} end {self.replay_end}')

        #recovering_session_flag = False
        if self.autorecover:
            self.recover_session()

        self.kafkacons.assign([TopicPartition('kafkavents', partition=0,
                                              offset=self.listen_offset)])
        self.kafkacons.commit()
        print(f'Conversing with Kafka {self.bootstrap_servers} '
              f'on topic {self.topic}')

        print(f'Listening at offset {self.listen_offset} ...')
        try:
            while True:
                kevent = self.kafkacons.poll(1.0)
                if kevent is None:
                    # nothing to see here. just waiting for a message.
                    continue
                elif kevent.error():
                    print('ERROR: {}'.format(kevent.error()))
                else:
                    print(f'KEVENT HEADERS: {kevent.headers()}')
                    topic = kevent.topic()
                    message_offset = kevent.offset()
                    partition = kevent.partition()
                    print(f'\nTOPIC: {topic} PARTITION: {partition} '
                          f'OFFSET: {message_offset}')

                    # Something occurred. Check event.
                    event = Event(kevent)
                    event.offset = message_offset
                    packet = event.packet
                    print(f'PACKET: {packet}')

                    sessionid = event.sessionid
                    #print(f'SESSIONID: {event.sessionid}')
                    if self.replay:
                        sessionid += '-replay'

                    if sessionid is not None:
                        if self.sessions.get(sessionid, None) is None:
                            self.sessions[sessionid] = \
                                ReportPortalSession(
                                    sessionid,
                                    topic=self.topic,
                                    offset_start=event.offset,
                                    datastore=self.datastore,
                                    reportportal=self.reportportal)

                    self.sessions[sessionid].handle_event(event)

                    print(f'SESSIONS: {self.sessions}')

                    if self.replay and message_offset == self.replay_end:
                        print('REPLAY COMPLETE. EXITING.')

                        sys.exit(0)
        except KeyboardInterrupt:
            print('Someone stopped all of the fun with the keyboard!')
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
        #KV_OFFSET
        #KV_RESUME
        KV_DATASTORE
        KV_TOPIC
    """
    kafka = KafkaventsReportPortal()
    kafka.listen()


if __name__ == '__main__':
    main()

# TODO: listen to multiple topics
# TODO: add if DEBUG to a lot of print statements
