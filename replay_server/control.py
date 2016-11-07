import asyncio
import logging
from aiohttp import web

from .server import ReplayServer


logger = logging.getLogger(__name__)


class ControlServer:
    def __init__(self, replay_server: ReplayServer):
        self.replay_server = replay_server

    def clients(self, request):
        body = repr(list(self.replay_server.clients)).encode()
        return web.Response(body=body, content_type='application/json')

    def run(self, loop):
        self.loop = loop

        port = 4040
        app = web.Application(loop=loop)
        app.router.add_route('GET', '/clients', self.clients)

        coro = loop.create_server(app.make_handler(), '127.0.0.1', port)
        logger.info('Control Server listening on %s:%s', self.replay_server.address, port)
        self.server = loop.run_until_complete(coro)




@asyncio.coroutine
def init(loop, replay_server: ReplayServer):
    """
    Initialize the http control server
    """
    ctrl_server = ControlServer(replay_server)
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/clients', ctrl_server.clients)

    port = 4040
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', port)
    logger.info("Control server listening on http://127.0.0.1:" + str(port))
    return srv
