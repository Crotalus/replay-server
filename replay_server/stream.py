import time
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

class ReplayStream:
    # pylint: disable=too-many-instance-attributes

    def __init__(self, game_id):
        log.debug("Init new replay_stream, id: %s", game_id)

        self.game_id = game_id

        self.header = None  # Replay Header Dict
        self.header_data = None  # Replay Header binary

        self.header_received = asyncio.Event()  # Set when header received

        self.steps = []  # All steps
        self.step = 0  # Beat id

        self.complete = False
        self.started_at = time.time()
        self.ended_at = None
        self.desynced = False

        self.streamers = set()
        self.peers = set()

        self._debug_streamer = None

    @property
    def info(self):
        return dict(started_at=self.started_at, ended_at=self.ended_at, complete=self.complete, desynced=self.desynced, featured_mod='faf', ticks=self.step, uid=self.game_id)

    @property
    def map(self):
        return self.header.map

    @property
    def map_name(self):
        return self.header.scenario['name']

    def __str__(self):
        return 'Stream(%s, %s (%s))' \
               % (self.game_id, self.map, self.map_name)

    def st_assert(self, condition, message=None):
        if not condition:
            self.desynced = True
            raise ReplayStreamDiverged(message)


    async def stream_steps(self, to_peer: ReplayPeer):
        "Stream replay to_peer"

        with keepref(to_peer, self.peers):
            await asyncio.wait_for(self.header_received.wait(), None)

            log.info('%s -> %s started. [%d peers]',
                     self, to_peer, len(self.peers))
            to_peer.send(self.header_data)

            while True:
                to_step = self.step
                if to_step > 1 and not self.complete and isinstance(to_peer, ReplayPeer):
                    to_step -= LIVE_DELAY

                for step_id in range(to_peer.sent_step, to_step):
                    step = self.steps[step_id]
                    # Need debug spam? Take these:
                    # log.debug('Step-data: %s', step.to_bytes())
                    to_peer.send(step.to_bytes())

                if self.complete:
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
            self.header_received.set()
        else:
            # header['random'] = self.header['random']  # XXX: for restart debugging
            self.st_assert(header == self.header, 'Header difference.')

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

            self.st_assert(self.steps[step.tick - 1] == step, 'Step difference.')
        else:
            step._debug_streamer = str(streamer)
            self.steps.append(step)
            self.step += 1

            if self.step % 600 == 0:
                log.debug('%s: current step = %d', self, self.step)

        if step.game_ender:
            self.complete = True
            self.ended_at = time.time()

            log.info('%s: stream finished. [%d peers]', self, len(self.peers))

    def end(self):
        if not self.ended_at:
            self.ended_at = time.time()
