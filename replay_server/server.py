import logging

import asyncio
from asyncio import StreamReader, StreamWriter

from os.path import join as pjoin
from weakref import WeakValueDictionary
from .peer import ReplayPeer, ReplayFilePeer
from .streamer import ReplayStreamer
from .stream import ReplayStream
from .util import keepref

import config

log = logging.getLogger(__name__)


class UnknownReplay(Exception):
    pass


class UnknownMethod(Exception):
    pass


class Client:
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer
        self.address = writer.get_extra_info('peername')

    async def read(self, n):
        data = await self.reader.read(n) if n > 0 else b''
        return data

    def write(self, data):
        self.writer.write(data)

    def close(self):
        self.writer.close()

class ReplayServer:
    def __init__(self, address: str, port):
        self.address = address
        self.port = port
        self.clients = set()
        self.replay_streams = WeakValueDictionary()
        self.replay_streamers = set()
        self.replay_peers = set()

    def run(self, loop):
        log.info('Live Replay Server listening on %s:%s', self.address, self.port)
        self.loop = loop
        coro = asyncio.start_server(self.connect_handler, self.address, self.port, loop=self.loop)
        self.server = loop.run_until_complete(coro)

        #self.server.init_socket()
        #self.server.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

    def stop(self):
        log.info('Shutting down gracefully.')
        self.server.close()
        log.info('Listen socket closed.')
        self.loop.run_until_complete(self.server.wait_closed())
        log.info('All streams finished.')

    def create_stream(self, game_id):
        "Gets or creates stream for posting."
        stream = self.replay_streams.get(game_id)

        if not stream:
            stream = ReplayStream(game_id)
            self.replay_streams[game_id] = stream

            file_peer = ReplayFilePeer(game_id)
            asyncio.ensure_future(stream.stream_steps(file_peer))

        return stream

    async def connect_handler(self, client_reader, client_writer):
        client = Client(client_reader, client_writer)
        log.info('Connection from %s. [%3d clients]', client.address, len(self.clients))
        self.clients.add(client)
        await self.handle_client(client)

    async def handle_client(self, client):
        async def readNulString(client):
            # if running 3.5.2
            #data = await client.reader.readuntil(b'\x00')
            data = b''
            c = await client.reader.read(1)
            while c and c != b'\x00':
                data = data + c
                c = await client.reader.read(1)

            return data.decode()

        try:
            # gpgnet:// old-style connection
            gpg_head = await readNulString(client)
            replay_name = gpg_head[1:].lower()

            game_id = int(replay_name.split("/")[1])

            if gpg_head[0] == 'P': # 'P'osting
                log.info('%s POST %s', client.address, replay_name)

                if replay_name.endswith(".gwreplay"):
                    galactic_war = True
                elif replay_name.endswith('.fafreplay'):
                    galactic_war = False
                elif replay_name.endswith(".scfareplay"):
                    log.exception("Can't handle .scfareplay: %s", replay_name)
                else:
                    log.exception('Unknown replay extension: %s', replay_name)

                stream = self.create_stream(game_id)

                streamer = ReplayStreamer(stream, client, replay_name, game_id)

                with keepref(streamer, self.replay_streamers):
                    try:
                        await streamer.read_stream()
                    except OSError:
                        pass # Disconnected

            elif gpg_head[0] == 'G': # 'G'etting
                log.info('%s GET %s', client.address, replay_name)

                stream = self.replay_streams.get(game_id)

                if not stream:
                    raise UnknownReplay('%s requested unknown replay: %s'
                                        % (client.address, replay_name))

                log.info('Connecting %s to %s', client.address, stream)
                peer = ReplayPeer(client)

                with keepref(peer, self.replay_peers):
                    try:
                        await stream.stream_steps(peer)
                    except BrokenPipeError:
                        pass # Disconnected
            else:
                raise UnknownMethod('%s unknown method: %s' % (client.address, gpg_head))
        except Exception:
            raise
        finally:
            client.close()
            self.clients.remove(client)
            log.info('%s disconnected. [%3d clients]', client.address, len(self.clients))
