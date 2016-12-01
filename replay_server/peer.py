import os
import logging
import asyncio
import json
import config

from .replayfile import ReplayFile

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

    def __init__(self, stream):
        self.stream = stream
        self.file = open(self.streaming_replay_path, 'wb')
        self.sent_step = -1  # Last step sent, -1 due to header

    def __str__(self):
        return 'ReplayFile(%s)' % self.file.name

    @property
    def game_id(self):
        return self.stream.game_id

    @property
    def streaming_replay_path(self):
        return os.path.join(config.STREAMING_FOLDER, '%d.scfareplay' % self.game_id)

    @property
    def info_path(self):
        return os.path.join(config.STREAMING_FOLDER, '%d.json' % self.game_id)

    def send(self, step_data):
        self.file.write(step_data)
        self.sent_step += 1
        if config.FLUSH_INTERVAL > 0 and not self.sent_step % (config.FLUSH_INTERVAL * 10):
            self.file.flush()
            self.save_infofile(self.get_streaminfo())

    def finish(self):
        self.file.close()
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.persist_replay(), loop=loop)

    def get_streaminfo(self):
        return self.stream.info

    def save_infofile(self, info):
        with open(self.info_path, 'w') as file:
            json.dump(info, file)

    async def persist_replay(self):
        replay = ReplayFile(self.streaming_replay_path, self.get_streaminfo())
        await replay.persist()
