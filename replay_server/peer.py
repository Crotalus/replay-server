import sys
import os
import asyncio
import zlib
import zipfile
import base64
import json
import logging

import aiomysql

import config
import db

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
    def __init__(self, stream):
        self.stream = stream
        self.file = open(self.streaming_path, 'wb')
        self.sent_step = -1  # Last step sent, -1 due to header

    def __str__(self):
        return 'Peer( %s )' % self.file.name

    @property
    def game_id(self):
        return self.stream.game_id

    @property
    def nestedpath(self):
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
    def streaming_path(self):
        return os.path.join(config.STREAMING_FOLDER, '%d.scfareplay' % self.game_id)

    @property
    def info_path(self):
        return os.path.join(config.STREAMING_FOLDER, '%d.json' % self.game_id)

    @property
    def pending_path(self):
        return os.path.join(config.PENDING_FOLDER, '%d.fafreplay' % self.game_id)
    @property
    def final_path(self):
        return os.path.join(self.nestedpath, '%d.fafreplay' % self.game_id)

    def send(self, step_data):
        self.file.write(step_data)
        self.sent_step += 1
        interval = config.FLUSH_INTERVAL
        if interval > 0 and not self.sent_step % (interval * 10):
            self.file.flush()

    def finish(self):
        self.file.close()
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.persist_replay(), loop=loop)

    def get_streaminfo(self):
        return dict(desynced=self.stream.desynced, featured_mod='faf', ticks=self.stream.step, uid=self.game_id)

    def save_infofile(self, info):
        with open(self.info_path, 'w') as file:
            json.dump(info, file)

    async def get_gameinfo(self):
        info = None

        pool = await db.get_pool()
        async with pool.get() as conn:
            cursor = await conn.cursor(aiomysql.DictCursor)

            query = """
                SELECT f.gamemod, gs.gameType, m.filename, gs.gameName, gps.playerId, gps.AI, gps.team
                FROM game_stats gs LEFT JOIN game_player_stats gps ON gps.gameId = gs.id LEFT JOIN table_map m ON m.id = gs.mapId
                LEFT JOIN game_featuredMods f ON gs.gameMod = f.id WHERE gs.id = '%s'
            """

            # Do we really need all these LEFT JOINs? Seems like we should try to keep table relations more sane
            """
                SELECT f.gamemod, gs.gameType, m.filename, gs.gameName, l.host, l.login, gps.playerId, gps.AI, gps.team
                FROM game_stats gs LEFT JOIN game_player_stats gps ON gps.gameId = gs.id LEFT JOIN table_map m ON m.id = gs.mapId
                LEFT JOIN logins l ON l.id = gps.playerid LEFT JOIN game_featuredMods f ON gs.gameMod = f.id WHERE gs.id = '%s'
            """

            await cursor.execute(query, self.game_id)
            row = await cursor.fetchone()
            if row is not None:
                info = row

        return info

    async def persist_replay(self):
        info = self.get_streaminfo()
        self.save_infofile(info)  # if something goes wrong, still keep a file with the json data

        ext_info = await self.get_gameinfo()
        if ext_info:
            info.update(ext_info)

        self.save_infofile(info)

        """
        if config.ASYNC_ZIP:
            await self.create_zipreplay_thread()
        else:
            self.create_zipreplay()
        """
        self.create_legacy_fafreplay()

        # we survived the fafreplay creation part, delete the temporary files
        os.remove(self.streaming_path)
        os.remove(self.info_path)

        await self.insert_replay()

        # in the future, we should use db transaction to make sure we can rollback if this rename fails for some reason
        os.rename(self.pending_path, self.final_path)

    def create_legacy_fafreplay(self):
        with open(self.info_path, 'r') as i_file, open(self.streaming_path, 'rb') as s_file, open(self.pending_path, 'wb') as p_file:
            infodata = i_file.read()
            replaydata = s_file.read()
            p_file.write((infodata + "\n").encode('utf-8'))
            p_file.write(base64.b64encode(zlib.compress(replaydata)))

    def create_fafreplay(self):
        with open(self.info_path, 'r') as i_file, open(self.streaming_path, 'rb') as s_file, open(self.pending_path, 'wb') as p_file:
            infodata = i_file.read()
            replaydata = s_file.read()
            p_file.write((infodata + "\n").encode('utf-8'))
            p_file.write(zlib.compress(replaydata))

    def create_zipreplay(self):
        with zipfile.ZipFile(self.pending_path, 'w') as z_file:
            z_file.write(self.info_path, os.path.basename(self.info_path))
            z_file.write(self.streaming_path, os.path.basename(self.streaming_path))

    async def create_zipreplay_thread(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.create_zipreplay)

    async def insert_replay(self):
        pool = await db.get_pool()
        async with pool.get() as conn:
            cursor = await conn.cursor()
            await cursor.execute("INSERT INTO game_replays (uid, ticks, desynced) VALUES ('%s', '%s', '%s')", (self.game_id, self.stream.step, self.stream.desynced))


