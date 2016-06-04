import logging

log = logging.getLogger(__name__)

class ReplayPeer:
    def __init__(self, client):
        self.client = client
        self.address = self.client.address

        self.sent_step = -1  # Last step sent, -1 due to header

    def __str__(self):
        return 'Peer%s' % (self.address,)

    def send(self, step_data):
        self.client.write(step_data)
        self.sent_step += 1

    def finish(self):
        self.client.writer.write_eof()


class ReplayFilePeer:
    def __init__(self, file):
        self.file = file

        self.sent_step = -1  # Last step sent, -1 due to header

    def __str__(self):
        return 'Peer( %s )' % self.file.name

    def send(self, step_data):
        self.file.write(step_data)
        self.sent_step += 1

    def finish(self):
        # TODO: Notify hook for a finished replay.
        self.file.close()
