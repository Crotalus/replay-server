import logging
import os
import time
import db
import aiomysql

log = logging.getLogger(__name__)


class DBService:
    def __init__(self, pool: aiomysql.Pool):
        self.pool = pool

    async def get_pool(self):
        if not self.pool:
            self.pool = await db.get_pool()

        return self.pool

    async def get_conn(self):
        pool = await self.get_pool()
        return pool.get()

    @property
    async def connection(self):
        pool = await self.get_pool()
        return pool.get()

    async def get_gameinfo(self, replay):
        async with await self.connection as conn:
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

            log.debug("Get gameinfo about replay %d from database", replay.id)
            await cursor.execute(query, replay.info['uid'])
            row = await cursor.fetchone()
            if row is not None:
                info = {
                    'featured_mod': row['gamemod'],
                    'game_type': row['gameType'],
                    'title': row['gameName'],
                }

                info['mapname'] = os.path.splitext(os.path.basename(row['filename']))[0]
                table = "updates_" + str(row['gamemod'])
                query = "SELECT fileId, MAX(version) as version FROM `%s` LEFT JOIN %s ON `fileId` = %s.id GROUP BY fileId"
                await cursor.execute(query, table + '_files', table, table)
                for r in cursor:
                    info['featured_mod_versions'][r['fileId']] = r['version']
                return info

        return None

    async def insert_replay(self, replay):
        log.debug("Insert replay %d into database", replay.id)
        async with await self.connection as conn:
            cursor = await conn.cursor()
            await cursor.execute("INSERT INTO game_replays (uid, ticks, complete, desynced, max_watchers) VALUES ('%s', '%s', '%s', '%s', '%s')", (replay.info['uid'], replay.info['ticks'], replay.info['complete'], replay.info['desynced'], replay.info['max_watchers']))
