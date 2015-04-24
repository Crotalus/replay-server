
import gevent
from gevent.event import Event

import logging
log = logging.getLogger(__name__)

from .data import *
from .peer import ReplayPeer
from .streamer import ReplayStreamer
from .util import keepref

from config import LIVE_DELAY

class ReplayStreamDiverged(Exception):
    pass

def STAssert(condition, message=None):
    "Stream Assert"
    if not condition:
        raise ReplayStreamDiverged(message)

class ReplayStream:
    def __init__(self, game_id):
        self.game_id = game_id

        self.header = None # Replay Header Dict
        self.header_data = None # Replay Header binary

        self.header_ev = Event() # Set when header received

        self.steps = [] # All steps
        self.step = 0 # Beat id

        self.game_ended = False

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

    def stream_steps(self, to_peer: ReplayPeer):
        "Stream replay to_peer"

        with keepref(to_peer, self.peers):
            self.header_ev.wait()

            log.info('%s -> %s started. [%d peers]',
                     self, to_peer, len(self.peers))

            to_peer.send(self.header_data)

            while 1:
                last_run = self.game_ended
                to_step = self.step
                if not self.game_ended:
                    to_step -= LIVE_DELAY

                for step_id in range(to_peer.sent_step, to_step):
                    step = self.steps[step_id]

                    # Need debug spam? Take these:
                    # log.debug('Sending #%d to %s from %s', step_id, to_peer, self)
                    # log.debug('Step-data: %s', step.to_bytes())
                    to_peer.send(step.to_bytes())

                if last_run:
                    to_peer.finish()
                    break

                gevent.sleep(0.1)

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
        print('FA HEAD:')
        print(header)
        if not self.header:
            self.header = header
            self.header_data = header_bin
            self.header_ev.set()
        else:
            STAssert(header == self.header, 'Header difference.')

    def push_step(self, streamer, step):
        assert isinstance(step, ReplayStep)

        if step.game_ender and len(self.streamers) > 1:
            log.debug("Dropping %s from %s", step, streamer)
            return

        if self.step >= step.tick:
            cur_step = self.steps[step.tick-1]
            if cur_step != step:
                print('From:', cur_step._debug_streamer, 'and', streamer)
                cur_step.debug_cmp(step)
            # STAssert(self.steps[step.tick-1] == step, 'Step difference.')
        else:
            step._debug_streamer = str(streamer)
            self.steps.append(step)

            self.step += 1

            if self.step % 100 == 0:
                log.debug('%s: current step = %d', self, self.step)

        if step.game_ender:
            self.game_ended = True

            log.info('%s: stream finished. [%d peers]', self, len(self.peers))


