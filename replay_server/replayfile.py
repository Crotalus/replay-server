import os
import asyncio
import json
import base64
import zlib
import zipfile
import logging
from struct import pack
import config
from .dbservice import DBService

log = logging.getLogger(__name__)


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

    def streaming_file(self, ext='scfareplay'):
        return self._build_path(config.STREAMING_FOLDER, ext)

    def final_file(self, ext='fafreplay'):
        return self._build_path(self.nestedpath, ext)

    async def persist(self):
        log.info('Persisting replay %d', self.id)

        if os.path.isfile(self.final_file()):
            log.debug("%s already exists", self.final_file())
            return

        # Move replay/info from streaming/ -> pending/ folder, XXX: check if not running
        s_file = self.streaming_file('scfareplay')
        i_file = self.streaming_file('json')
        if os.path.isfile(s_file):
            if not self.info:
                self.load_info(i_file)
            else:
                self.save_info(i_file)

            os.rename(s_file, self.pending_file('scfareplay'))
            os.rename(i_file, self.pending_file('json'))

        db_service = DBService()
        # Create fafreplay in pending/
        fafreplay = self.pending_file('fafreplay')
        if not os.path.isfile(fafreplay):
            i_file = self.pending_file('json')
            if not self.info:
                self.load_info(i_file)

            if 'featured_mod' not in self.info:
                ext_info = await db_service.get_gameinfo(self)
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
            os.remove(self.pending_file('scfareplay'))

        await db_service.insert_replay(self)

        # in the future, we should use db transaction to make sure we can
        # rollback if this rename fails for some reason
        os.rename(fafreplay, self.final_file())

    def create_fafreplay(self, legacy=True):
        infofile = self.pending_file('json')
        screplay = self.pending_file('scfareplay')
        fafreplay = self.pending_file('fafreplay')

        with open(infofile, 'r') as ifile, open(screplay, 'rb') as sc, open(fafreplay, 'wb') as faf:
            info = ifile.read()
            faf.write((info + "\n").encode('utf-8'))
            replaydata = zlib.compress(sc.read())
            if legacy:
                #  legacy format uses base64 encoded zipstream with 4 bytes (big endian) data length header
                replaysize = os.fstat(sc.fileno()).st_size
                replaydata = pack('>I', replaysize) + replaydata
                faf.write(base64.b64encode(replaydata))
            else:
                faf.write(replaydata)

    def create_zipreplay(self):
        infofile = self.pending_file('json')
        screplay = self.pending_file('scfareplay')
        fafreplay = self.pending_file('fafreplay')

        with zipfile.ZipFile(fafreplay, 'w') as z_file:
            z_file.write(infofile,
                         os.path.basename(infofile))
            z_file.write(screplay,
                         os.path.basename(screplay))

    async def create_zipreplay_thread(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.create_zipreplay)
