import asyncio
import aiomysql
import logging
import os
import zlib
import base64
import json
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

    def finish(self):
        self.file.close()
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.create_fafreplay(), loop=loop)

    def get_streaminfo(self):
        return dict(desynced=self.stream.desynced, featured_mod='faf', ticks=self.stream.step, uid=self.game_id)

    def save_streaminfo(self):
        with open(self.info_path, 'w') as file:
            json.dump(self.get_streaminfo(), file)

    async def get_gameinfo(self):
        info = None

        pool = await db.get_pool()
        async with pool.get() as conn:
            cursor = await conn.cursor(aiomysql.DictCursor)

            """
                SELECT f.gamemod, gs.gameType, m.filename, gs.gameName, gps.playerId, gps.AI, gps.team
                FROM game_stats gs LEFT JOIN game_player_stats gps ON gps.gameId = gs.id LEFT JOIN table_map m ON m.id = gs.mapId
                LEFT JOIN game_featuredMods f ON gs.gameMod = f.id WHERE gs.id = '%s'
            """

            # Do we really need all these LEFT JOINs? Seems like we should try to keep table relations more sane
            query = """
                SELECT f.gamemod, gs.gameType, m.filename, gs.gameName, l.host, l.login, gps.playerId, gps.AI, gps.team
                FROM game_stats gs LEFT JOIN game_player_stats gps ON gps.gameId = gs.id LEFT JOIN table_map m ON m.id = gs.mapId
                LEFT JOIN logins l ON l.id = gps.playerid LEFT JOIN game_featuredMods f ON gs.gameMod = f.id WHERE gs.id = '%s'
            """

            await cursor.execute(query, self.game_id)
            row = await cursor.fetchone()
            if row is not None:
                info = row

        return info

    async def create_fafreplay(self):
        self.save_streaminfo()  # if something goes wrong, still keep a file with the json data
        info = self.get_streaminfo()

        ext_info = await self.get_gameinfo()
        if ext_info:
            info.update(ext_info)

        with open(self.streaming_path, 'rb') as s_file, open(self.pending_path, 'wb') as p_file:
            replaydata = s_file.read()
            p_file.write((json.dumps(info, ensure_ascii=False) + "\n").encode('utf-8'))
            p_file.write(zlib.compress(replaydata))

        # we survived the fafreplay creation part, delete the temporary stream files
        os.remove(self.streaming_path)
        os.remove(self.info_path)

        await self.insert_replay()

        # use db transaction to make sure we can rollback if this rename fails for some reason?
        os.rename(self.pending_path, self.final_path)

    async def insert_replay(self):
        pool = await db.get_pool()
        async with pool.get() as conn:
            cursor = await conn.cursor()
            await cursor.execute("INSERT INTO game_replays (uid, ticks, desynced) VALUES ('%s', '%s', '%s')", (self.game_id, self.stream.step, self.stream.desynced))


