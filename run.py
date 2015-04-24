
import sys, os

if sys.version_info.major != 3:
    raise RuntimeError("FAForever Live Replay Server requires python 3.\n")

import gevent
import gevent.monkey

# Monkey-patch all standard libraries
# Only threading monkey-patch is required (for peewee).
gevent.monkey.patch_all()

import config

# ==== Initialize Logging ====
import logging

FORMAT = '%(asctime)-15s %(levelname)-8s %(name)-10s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

log = logging.getLogger(__name__)

# ==== Initialize Database ====

from peewee import MySQLDatabase
from db.faf_orm import *

db = MySQLDatabase(config.DATABASE)

faf_orm_init_db(db)

# ==== Initialize Server ====
from replay_server.server import ReplayServer

server = ReplayServer(config.LISTEN_ADDRESS)

from signal import SIGHUP

gevent.signal(SIGHUP, server.stop)

if not os.path.exists(config.REPLAY_FOLDER):
    log.info('Creating replay folder "%s"', config.REPLAY_FOLDER)
    os.mkdir(config.REPLAY_FOLDER)

log.info('Starting...')
server.run()
