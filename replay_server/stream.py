import asyncio
import logging

from .data import ReplayStep
from .peer import ReplayPeer
from .streamer import ReplayStreamer
from .util import keepref

from config import LIVE_DELAY

log = logging.getLogger(__name__)


class ReplayStreamDiverged(Exception):
    pass


def STAssert(condition, message=None):
    "Stream Assert"
    if not condition:
        raise ReplayStreamDiverged(message)


class ReplayStream:
    # pylint: disable=too-many-instance-attributes

    def __init__(self, game_id):
        log.debug("Init new replay_stream, id: %s", game_id)

        self.game_id = game_id

        self.header = None  # Replay Header Dict
        self.header_data = None  # Replay Header binary

        self.header_ev = asyncio.Event()  # Set when header received

        self.steps = []  # All steps
        self.step = 0  # Beat id

        self.game_ended = False
        self.desynced = False

        self.streamers = set()
        self.peers = set()

    @property
    def map(self):
        return self.header.map

    @property
    def map_name(self):
        return self.header.scenario['name']

    def __str__(self):
        return 'Stream(%s, %s (%s))' \
               % (self.game_id, self.map, self.map_name)

    async def stream_steps(self, to_peer: ReplayPeer):
        "Stream replay to_peer"

        with keepref(to_peer, self.peers):
            await asyncio.wait_for(self.header_ev.wait(), None)

            log.info('%s -> %s started. [%d peers]',
                     self, to_peer, len(self.peers))
            to_peer.send(self.header_data)

            while True:
                to_step = self.step
                if to_step > 1 and not self.game_ended and isinstance(to_peer, ReplayPeer):
                    to_step -= LIVE_DELAY

                for step_id in range(to_peer.sent_step, to_step):
                    step = self.steps[step_id]
                    # Need debug spam? Take these:
                    # log.debug('Step-data: %s', step.to_bytes())
                    to_peer.send(step.to_bytes())

                if self.game_ended:
                    to_peer.finish()
                    break

                await asyncio.sleep(0.1)

            log.info('%s -> %s finished. [%d peers]',
                     self, to_peer, len(self.peers))

    # ==== These are called by ReplayStreamer ====
    def add_streamer(self, streamer):
        assert isinstance(streamer, ReplayStreamer)

        self.streamers.add(streamer)

    def del_streamer(self, streamer):
        assert isinstance(streamer, ReplayStreamer)

        self.streamers.remove(streamer)

    def push_header(self, header, header_bin):
        if not self.header:
            self.header = header
            self.header_data = header_bin
            self.header_ev.set()
        else:
            # header['random'] = self.header['random']  # XXX: for restart debugging
            #log.debug("self.header:\n{0}\nheader:\n{1}\n".format(self.header, header))
            self.desynced = True
            STAssert(str(header) == str(self.header), 'Header difference.')

    def push_step(self, streamer, step):
        assert isinstance(step, ReplayStep)

        if step.game_ender and len(self.streamers) > 1:
            log.debug("Dropping %s from %s", step, streamer)
            return

        if self.step >= step.tick:
            cur_step = self.steps[step.tick - 1]
            if cur_step != step:
                log.debug("From:%s and %s", cur_step._debug_streamer, streamer)
                cur_step.debug_cmp(step)

            self.desynced = True
            STAssert(self.steps[step.tick - 1] == step, 'Step difference.')
        else:
            step._debug_streamer = str(streamer)

            self.steps.append(step)

            self.step += 1

            if self.step % 600 == 0:
                log.debug('%s: current step = %d', self, self.step)

        if step.game_ender:
            self.game_ended = True

            log.info('%s: stream finished. [%d peers]', self, len(self.peers))

    def end(self):
        self.game_ended = True
