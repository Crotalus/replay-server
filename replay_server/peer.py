import asyncio
import logging
import os
import zlib
import base64
import json
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
        self.file = open(self.sc_path, 'wb')
        self.sent_step = -1  # Last step sent, -1 due to header

    def __str__(self):
        return 'Peer( %s )' % self.file.name

    @property
    def dirpath(self):
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

        return dirname

    @property
    def sc_path(self):
        return os.path.join(self.dirpath, '%d.scfareplay' % self.game_id)

    @property
    def faf_path(self):
        return os.path.join(self.dirpath, '%d.fafreplay' % self.game_id)

    def send(self, step_data):
        self.file.write(step_data)
        self.sent_step += 1

    def finish(self):
        self.file.close()
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.create_fafreplay(), loop=loop)

    async def create_fafreplay(self):
        info = await self.get_gameinfo()

        with open(self.sc_path, 'rb') as sc_replay, open(self.faf_path, 'wb') as faf_replay:
            replaydata = sc_replay.read()
            faf_replay.write((json.dumps(info, ensure_ascii=False) + "\n").encode('utf-8'))
            faf_replay.write(zlib.compress(replaydata))

    async def get_gameinfo(self):
        return dict(info='here')
