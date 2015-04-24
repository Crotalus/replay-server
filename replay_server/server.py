
import gevent
from gevent.server import StreamServer

from os.path import join as pjoin

from socket import SOL_SOCKET, SO_REUSEADDR

from weakref import WeakValueDictionary

from .peer import ReplayPeer, ReplayFilePeer
from .streamer import ReplayStreamer
from .stream import ReplayStream
from .util import keepref

import config

import logging
log = logging.getLogger(__name__)

class UnknownReplay(Exception):
    pass
class UnknownMethod(Exception):
    pass

class ReplayServer:
    def __init__(self, bind_address):
        self.server = StreamServer(bind_address, self.connect_handler)
        self.server.init_socket()
        self.server.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

        self.clients = set()

        self.replay_streams = WeakValueDictionary()
        self.replay_streamers = set()
        self.replay_peers = set()

    def run(self):
        log.info('Live Replay Server listening on %s:%s',
                 self.server.server_host, self.server.server_port)
        self.server.serve_forever()

    def stop(self):
        log.info('Shutting down gracefully.')
        self.server.close()
        log.info('Listen socket closed.')
        self.server.stop()
        log.info('All streams finished.')

    def create_stream(self, game_id):
        "Gets or creates stream for posting."
        stream = self.replay_streams.get(game_id)

        if not stream:
            stream = ReplayStream(game_id)
            self.replay_streams[game_id] = stream

            file_peer = ReplayFilePeer(open(pjoin(config.REPLAY_FOLDER, '%d.scfareplay' % game_id), 'wb'))
            gevent.spawn(stream.stream_steps, file_peer)

        return stream

    def connect_handler(self, client_socket, client_address):
        self.clients.add(client_socket)

        log.info('Connection from %s. [%3d clients]', client_address, len(self.clients))

        def readNulString(socket):
            s = b''
            c = socket.recv(1)
            while c != b'\x00':
                s = s + c
                c = socket.recv(1)
            return s.decode()

        try:
            # gpgnet:// old-style connection
            gpg_head = readNulString(client_socket)
            replay_name = gpg_head[1:]

            game_id = int(replay_name.split("/")[1])

            if gpg_head[0] == 'P': # 'P'osting
                log.info('%s POST %s', client_address, replay_name)

                if replay_name.endswith(".gwreplay"):
                    galactic_war = True
                elif replay_name.endswith('.fafreplay'):
                    galactic_war = False
                elif replay_name.endswith(".scfareplay"):
                    log.exception("Can't handle .scfareplay: %s", replay_name)
                else:
                    log.exception('Unknown replay extension: %s', replay_name)

                stream = self.create_stream(game_id)

                streamer = ReplayStreamer(stream, client_socket, replay_name, game_id)

                with keepref(streamer, self.replay_streamers):
                    try:
                        streamer.read_stream()
                    except OSError:
                        pass # Disconnected

            elif gpg_head[0] == 'G': # 'G'etting
                log.info('%s GET %s', client_address, replay_name)

                stream = self.replay_streams.get(game_id)

                if not stream:
                    raise UnknownReplay('%s requested unknown replay: %s'
                                        % (client_address, replay_name))

                log.info('Connecting %s to %s', client_address, stream)
                peer = ReplayPeer(client_socket)

                with keepref(peer, self.replay_peers):
                    try:
                        stream.stream_steps(peer)
                    except BrokenPipeError:
                        pass # Disconnected
            else:
                raise UnknownMethod('%s unknown method: %s' % (client_address, gpg_head))
        finally:
            if not client_socket.closed:
                client_socket.shutdown(2)
            self.clients.remove(client_socket)
            log.info('%s disconnected. [%3d clients]', client_address, len(self.clients))