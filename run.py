import sys
import os
import asyncio
import logging
import config

from replay_server.server import ReplayServer

if sys.version_info.major != 3:
    raise RuntimeError("FAForever Live Replay Server requires python 3.\n")

FORMAT = '%(asctime)-15s %(levelname)-8s %(name)-10s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

log = logging.getLogger(__name__)

# ==== Initialize Database ====

#from peewee import MySQLDatabase
#from db.faf_orm import *

#db = MySQLDatabase(config.DATABASE)

#faf_orm_init_db(db)

# ==== Initialize Server ====

# Set to False for now, ProactorEventLoop and asyncio not playing well due to:
# http://bugs.python.org/issue26819
if sys.platform == 'win32' and False:
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
else:
    loop = asyncio.get_event_loop()
#loop.set_debug('enabled')

server = ReplayServer(config.LISTEN_ADDRESS, config.LISTEN_PORT)

for f in [config.REPLAY_FOLDER, config.STREAMING_FOLDER, config.PENDING_FOLDER]:
    if not os.path.exists(f):
        log.info('Creating folder "%s"', f)
        os.makedirs(f)

log.info('Starting...')
server.run(loop)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.stop()
loop.close()
