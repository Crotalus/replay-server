import asyncio
import aiomysql
from aiomysql import Pool
from .logging_cursor import LoggingCursor

from config import DATABASE

db_pool = None

""" Migrations:
ALTER TABLE game_replays CHANGE UID uid bigint;
ALTER TABLE game_replays ADD COLUMN ticks int NOT NULL;
ALTER TABLE game_replays ADD COLUMN desynced boolean default false;
"""

async def get_pool(loop=None):
    global db_pool

    if not db_pool:
        await connect(loop=loop)

    return db_pool


def set_pool(pool: Pool):
    """
    Set the globally used pool to the given argument
    """
    global db_pool
    db_pool = pool


async def connect(host=DATABASE['host'], port=DATABASE['port'], user=DATABASE['user'], password=DATABASE['password'], db=DATABASE['db'],
            minsize=1, maxsize=1, cursorclass=LoggingCursor, loop=None):
    """
    Initialize the database pool
    :param loop:
    :param host:
    :param user:
    :param password:
    :param db:
    :param minsize:
    :param maxsize:
    :param cursorclass:
    :return:
    """

    if loop is None:
        loop = asyncio.get_event_loop()

    pool = await aiomysql.create_pool(host=host,
                                           port=port,
                                           user=user,
                                           password=password,
                                           db=db,
                                           autocommit=True,
                                           loop=loop,
                                           minsize=minsize,
                                           maxsize=maxsize,
                                           cursorclass=cursorclass)
    set_pool(pool)
    return pool
