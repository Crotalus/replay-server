from struct import unpack
import logging

from .data import *
from .header import parseHeader, DataStream

log = logging.getLogger(__name__)


class ReplayStreamer:

    def __init__(self, stream, client, replay_name, game_id):
        self.game_id = game_id
        self.name = replay_name[replay_name.find('/', 1) + 1:]

        self.stream = stream

        self.client = client
        self.address = self.client.address

        self.step_buffer = []  # Buffers a step

        self.header = None  # Replay Header Dict
        self.header_data = None  # Replay Header binary

        self.steps = []  # All steps

        self.step = 0  # Beat id

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
        assert n_beats == 1  # n_beats might be used in future

        step = ReplayStep(self.step + 1, self.step_buffer)
        self.steps.append(step)

        self.step += 1
        self.step_buffer = []

        self.stream.push_step(self, step)

        if self.step % 600 == 0:
            log.debug('%s:\ncurrent step = %d', self, self.step)

    async def _read(self, nbytes):
        ret = await self.client.reader.read(nbytes)
        if len(ret) == 0:
            # Socket closed
            raise OSError()
        return ret

    async def read_header(self):
        ds = DataStream(self.client.reader)
        self.header = await parseHeader(ds)
        self.header_data = ds.data()

    async def read_operation(self):
        data = await self._read(3)
        op, op_len = unpack('<BH', data)

        if op_len > 3:
            data = await self._read(op_len - 3)
        else:
            data = b''

        return CMDST_Operation(op, data)

    async def read_stream(self):
        await self.read_header()
        self.stream.push_header(self.header, self.header_data)

        while 1:
            op = await self.read_operation()
            self.step_buffer.append(op)
            if op.op in [CMDST.Advance, CMDST.SingleStep]:
                # operations that flush the steps
                self.advance_step(1)

            if op.op == CMDST.EndGame:
                self.advance_step(1)
                break
