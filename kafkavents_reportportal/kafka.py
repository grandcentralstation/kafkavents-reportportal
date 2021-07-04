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
"""Kafka connection module."""
from confluent_kafka import Consumer, TopicPartition


class Kafka:
    """Kafka class for handling Kafka connection."""

    def __init__(self, config):
        """Initialize the Kafka connection."""
        self.config = config
        self._topic = None
        self._consumer = None

    @property
    def topic(self):
        """Get Kafka topic."""
        return self._topic

    @topic.setter
    def topic(self, topic):
        if self.consumer is not None:
            self.consumer.subscribe([topic])

        self._topic = topic

    @property
    def group_id(self):
        """Get the group_id."""
        return self.config['group.id']

    @group_id.setter
    def group_id(self, group_id):
        self.config['group.id'] = group_id

    @property
    def consumer(self):
        """Get the Kafka connection."""
        return self._consumer

    @consumer.setter
    def consumer(self, consumer):
        self._consumer = consumer

    @property
    def bootstrap_servers(self):
        """Get bootstrap servers."""
        return self.config['bootstrap.servers']

    @bootstrap_servers.setter
    def bootstrap_servers(self, bootstrap_servers):
        self.config['bootstrap_servers'] = bootstrap_servers

    @property
    def client_id(self):
        """Get client id."""
        return self.config['client.id']

    @client_id.setter
    def client_id(self, client_id):
        self.config['client.id'] = client_id

    def connect(self):
        """Connect to the Kafka instance."""
        self.consumer = Consumer(self.config)

        return self.consumer
