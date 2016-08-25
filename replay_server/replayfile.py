import os
import asyncio
import json
import base64
import zlib
import zipfile
import logging
import aiomysql

import db

import config


class ReplayFile:

    def __init__(self, file, info=None):
        self.id = int(os.path.splitext(os.path.basename(file))[0])
        self.info = info

    def load_info(self, file):
        with open(file, 'r') as fd:
            self.info = json.load(fd)

    def save_info(self, file):
        with open(file, 'w') as fd:
            json.dump(self.info, fd)

    @property
    def nestedpath(self):
        # copypasted this mess from old replay server
        path = config.REPLAY_FOLDER
        dirsize = 100
        depth = 5
        i = depth
        dirname = path
        while i > 1:
            dirname = os.path.join(dirname, str(
                (self.id // (dirsize ** (i - 1))) % dirsize))
            i = i - 1

        if not os.path.exists(dirname):
            os.makedirs(dirname)

        return dirname

    def _build_path(self, path, ext):
        return os.path.join(path, '{0}.{1}'.format(self.id, ext))

    def pending_file(self, ext='fafreplay'):
        return self._build_path(config.PENDING_FOLDER, ext)

    def streaming_file(self, ext='screplay'):
        return self._build_path(config.STREAMING_FOLDER, ext)

    def final_file(self, ext='fafreplay'):
        return self._build_path(self.nestedpath, ext)

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

            # Do we really need all these LEFT JOINs? Seems like we should try
            # to keep table relations more sane
            """
                SELECT f.gamemod, gs.gameType, m.filename, gs.gameName, l.host, l.login, gps.playerId, gps.AI, gps.team
                FROM game_stats gs LEFT JOIN game_player_stats gps ON gps.gameId = gs.id LEFT JOIN table_map m ON m.id = gs.mapId
                LEFT JOIN logins l ON l.id = gps.playerid LEFT JOIN game_featuredMods f ON gs.gameMod = f.id WHERE gs.id = '%s'
            """

            await cursor.execute(query, self.info.uid)
            row = await cursor.fetchone()
            if row is not None:
                info = row

        return info

    async def insert_replay(self):
        pool = await db.get_pool()
        async with pool.get() as conn:
            cursor = await conn.cursor()
            await cursor.execute("INSERT INTO game_replays (uid, ticks, desynced) VALUES ('%s', '%s', '%s')", (self.info.uid, self.info.ticks, self.info.desynced))

    async def persist(self):
        if os.path.isfile(self.final_file()):
            return

        # Move replay /info from streaming/ -> pending/ folder, XXX: check if not running
        s_file = self.streaming_file('screplay')
        i_file = self.streaming_file('json')
        if os.path.isfile(s_file):
            if not self.info:
                self.load_info(i_file)
            else:
                self.save_info(i_file)

            os.rename(s_file, self.pending_file('screplay'))
            os.rename(i_file, self.pending_file('json'))

        # Create fafreplay in pending/
        fafreplay = self.pending_file('fafreplay')
        if not os.path.isfile(fafreplay):
            i_file = self.pending_file('json')
            if not self.info:
                self.load_info(i_file)

            if True:  # XXX: ext_info check here
                ext_info = await self.get_gameinfo()
                if ext_info:
                    self.info.update(ext_info)
                    self.save_info(i_file)

            """
            if config.ASYNC_ZIP:
                await self.create_zipreplay_thread()
            else:
                self.create_zipreplay()
            """
            self.create_fafreplay()
            os.remove(i_file)
            os.remove(self.pending_file('screplay'))

        await self.insert_replay()

        # in the future, we should use db transaction to make sure we can
        # rollback if this rename fails for some reason
        os.rename(fafreplay, self.final_file())

    def create_fafreplay(self, legacy=True):
        infofile = self.pending_file('json')
        screplay = self.pending_file('screplay')
        fafreplay = self.pending_file('fafreplay')

        with open(infofile, 'r') as ifile, open(screplay, 'rb') as sc, open(fafreplay, 'wb') as faf:
            info = ifile.read()
            faf.write((info + "\n").encode('utf-8'))
            if legacy:
                #  legacy format uses base64 encoding
                faf.write(base64.b64encode(zlib.compress(sc.read())))
            else:
                faf.write(zlib.compress(sc.read()))

    def create_zipreplay(self):
        infofile = self.pending_file('json')
        screplay = self.pending_file('screplay')
        fafreplay = self.pending_file('fafreplay')

        with zipfile.ZipFile(fafreplay, 'w') as z_file:
            z_file.write(infofile,
                         os.path.basename(infofile))
            z_file.write(screplay,
                         os.path.basename(screplay))

    async def create_zipreplay_thread(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.create_zipreplay)
