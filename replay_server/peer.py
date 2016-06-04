import logging
import os
import config

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
    def __init__(self, game_id):
        self.game_id = game_id
        self.file = None
        self.sent_step = -1  # Last step sent, -1 due to header
        self.open_file()

    def __str__(self):
        return 'Peer( %s )' % self.file.name

    @property
    def filepath(self):
        # copypasted this mess from old replay server
        path = config.REPLAY_FOLDER
        dirsize = 100
        depth = 5
        i = depth
        dirname = path
        while i > 1:
            dirname = os.path.join(dirname, str((self.game_id // (dirsize ** (i-1))) % dirsize))
            i = i - 1

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        return os.path.join(dirname, '%d.scfareplay' % self.game_id)

    def open_file(self):
        self.file = open(self.filepath, 'wb')

    def send(self, step_data):
        self.file.write(step_data)
        self.sent_step += 1

    def finish(self):
        # TODO: Notify hook for a finished replay.
        self.file.close()
