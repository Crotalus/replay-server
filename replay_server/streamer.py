from struct import unpack
import logging

from .data import *
from .header import parseHeader, DataStream

log = logging.getLogger(__name__)


class ReplayStreamer:
    def __init__(self, stream, streamer_socket, replay_name, game_id):
        self.game_id = game_id
        self.name = replay_name[replay_name.find('/', 1)+1:]

        self.stream = stream

        self.socket = streamer_socket
        self.address = self.socket.getpeername()

        self._file = self.socket.makefile('rb')

        self.step_buffer = [] # Buffers a step

        self.header = None # Replay Header Dict
        self.header_data = None # Replay Header binary

        self.steps = [] # All steps

        self.step = 0 # Beat id

        self.game_ended = False

    @property
    def map(self):
        return self.header.map

    @property
    def map_name(self):
        return self.header.scenario['name']

    def __str__(self):
        return 'Streamer(%s, %d, %s, %s (%s))' % \
               (self.address, self.game_id,
                self.name, self.map, self.map_name)

    def advance_step(self, n_beats):
        assert n_beats == 1 # n_beats might be used in future

        step = ReplayStep(self.step+1, self.step_buffer)
        self.steps.append(step)

        self.step += 1
        self.step_buffer = []

        self.stream.push_step(self, step)

        if self.step % 100 == 0:
            log.debug('%s: current step = %d', self, self.step)

    def _read(self, nbytes):
        ret = self._file.read(nbytes)
        if len(ret) == 0:
            # Socket closed
            raise OSError()
        return ret

    def read_header(self):
        ds = DataStream(self._file)
        self.header = parseHeader(ds)
        self.header_data = ds.data()

    def read_operation(self):
        op, op_len = unpack('<BH', self._read(3))

        data = self._read(op_len - 3)

        return CMDST_Operation(op, data)

    def read_stream(self):
        self.stream.add_streamer(self)
        try:
            self.read_header()
            self.stream.push_header(self.header, self.header_data)

            while 1:
                op = self.read_operation()

                self.step_buffer.append(op)

                if op.op in [CMDST.Advance, CMDST.SingleStep]:
                    # operations that flush the steps
                    self.advance_step(1)

                if op.op == CMDST.EndGame:
                    self.advance_step(1)

                    self.game_ended = True
                    break

            log.info('%s: stream finished.', self)
        finally:
            self.stream.del_streamer(self)