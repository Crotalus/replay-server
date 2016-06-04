"""
Functions and classes related to replay structure
"""

from struct import pack
from time import time

import logging

log = logging.getLogger(__name__)


class attrdict(dict):
    def __init__(self, *args, **kwargs):
        super(attrdict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class CMDST:
    Advance = 0
    SetCommandSource = 1
    CommandSourceTerminated = 2
    VerifyChecksum = 3
    RequestPause = 4
    Resume = 5
    SingleStep = 6
    CreateUnit = 7
    CreateProp = 8
    DestroyEntity = 9
    WarpEntity = 10
    ProcessInfoPair = 11
    IssueCommand = 12
    IssueFactoryCommand = 13
    IncreaseCommandCount = 14
    DecreaseCommandCount = 15
    SetCommandTarget = 16
    SetCommandType = 17
    SetCommandCells = 18
    RemoveCommandFromQueue = 19
    DebugCommand = 20
    ExecuteLuaInSim = 21
    LuaSimCallback = 22
    EndGame = 23

ECmdStreamOp = attrdict({
    "CMDST_Advance": 0,
    "CMDST_SetCommandSource": 1,
    "CMDST_CommandSourceTerminated": 2,
    "CMDST_VerifyChecksum": 3,
    "CMDST_RequestPause": 4,
    "CMDST_Resume": 5,
    "CMDST_SingleStep": 6,
    "CMDST_CreateUnit": 7,
    "CMDST_CreateProp": 8,
    "CMDST_DestroyEntity": 9,
    "CMDST_WarpEntity": 10,
    "CMDST_ProcessInfoPair": 11,
    "CMDST_IssueCommand": 12,
    "CMDST_IssueFactoryCommand": 13,
    "CMDST_IncreaseCommandCount": 14,
    "CMDST_DecreaseCommandCount": 15,
    "CMDST_SetCommandTarget": 16,
    "CMDST_SetCommandType": 17,
    "CMDST_SetCommandCells": 18,
    "CMDST_RemoveCommandFromQueue": 19,
    "CMDST_DebugCommand": 20,
    "CMDST_ExecuteLuaInSim": 21,
    "CMDST_LuaSimCallback": 22,
    "CMDST_EndGame": 23
})
ECmdStreamOp_R = {v: k for k,v in ECmdStreamOp.items()}

class CMDST_Operation:
    def __init__(self, op, data):
        self.op = op
        self.data = data

    def __eq__(self, other):
        return isinstance(other, CMDST_Operation) \
            and self.op == other.op and self.data == other.data

    def __str__(self):
        return '{0} ( {1} )'.format(ECmdStreamOp_R[self.op], self.data)

    def __bytes__(self):
        return pack('<BH', self.op, len(self.data)+3)+self.data

class ReplayStep:
    def __init__(self, tick_id, step_ops):

        self.tick = tick_id
        self.timestamp = time()
        self.operations = step_ops
        self.game_ender = step_ops[-1].op in [CMDST.EndGame]

    def __eq__(self, other):
        assert isinstance(other, ReplayStep)

        return self.tick == other.tick and self.operations == other.operations

    def debug_cmp(self, other):
        if self.tick != other.tick:
            log.debug('ReplayStep.eq - Tick mismatch: %d != %d' %
                  (self.tick, other.tick))
            return False

        if self.operations != other.operations:
            log.debug("ReplayStep.eq - Operations don't match:")
            for i in range(max(len(self.operations), len(other.operations))):
                try:
                    op_a = self.operations[i]
                except IndexError:
                    op_a = None
                try:
                    op_b = other.operations[i]
                except IndexError:
                    op_b = None

                if op_a != op_b:
                    print('%4x: %30s != %30s' % (i, op_a, op_b))
                else:
                    print('%4x: %30s == %30s' % (i, op_a, op_b))

            return False
        return True

    def to_bytes(self):
        "Get data blob"
        return b''.join([bytes(x) for x in self.operations])

    def __str__(self):
        return ''.join([str(o) for o in self.operations])
