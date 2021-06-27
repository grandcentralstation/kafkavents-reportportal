"""The module that handles all things Kafkavents to ReportPortal."""
import json


class RPBridgeData:
    """Provide a struct for the datastore."""

    def __init__(self):
        """Do nothing but wait for data."""
        pass


class RPBridge():
    """The Kafkavents bridge class managing ReportPortal sessions."""

    def __init__(self, sessionid, datastore='/tmp/kafkavents/datastore',
                 topic=None, offset_start=None):
        """Initialize the bridge."""
        self.datastore = datastore
        self.data = RPBridgeData()
        self.sessionid = sessionid
        self.offset_start = offset_start
        self.offset_end = None
        self.offset_last = None
        self.topic = None
        self.launch = None
        self.testnodes = {}

    @property
    def sessionid(self):
        """Session ID for KV to RP."""
        return self.data.sessionid

    @sessionid.setter
    def sessionid(self, sessionid):
        self.data.sessionid = sessionid
        self.write()

    @property
    def launch(self):
        """RP Launch for this session."""
        return self.data.launch

    @launch.setter
    def launch(self, launch):
        self.data.launch = launch
        self.write()

    @property
    def offset_end(self):
        """Kafka offset at end of session."""
        return self.data.offset_end

    @offset_end.setter
    def offset_end(self, offset):
        self.data.offset_end = offset
        self.write()

    @property
    def offset_last(self):
        """Kafka offset last read."""
        return self.data.offset_last

    @offset_last.setter
    def offset_last(self, offset):
        self.data.offset_last = offset
        self.write()

    @property
    def offset_start(self):
        """Kafka offset at start of session."""
        return self.data.offset_start

    @offset_start.setter
    def offset_start(self, offset):
        self.data.offset_start = offset
        self.write()

    @property
    def testnodes(self):
        """Test paths provided by PyTest nodeid in the Kafka event."""
        return self.data.testnodes

    @testnodes.setter
    def testnodes(self, testnodes):
        self.data.testnodes = testnodes
        self.write()

    def write(self):
        """Write the session to disk (or other future)."""
        # TODO: make this a lot more efficient than writing all data
        with open(f'{self.datastore}/{self.sessionid}.datastore', "w") as ch:
            json.dump(self.data.__dict__, ch, indent=2, sort_keys=True)

    def start(self):
        """Start the session."""

    def end(self):
        """End the session."""
        self.write()

    def resume(self):
        """Resume a session that was interrupted before completion."""
        pass

    def replay(self):
        """Replay a session from start to end."""
        pass
