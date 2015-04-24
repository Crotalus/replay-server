"""
GPG Replay Header parsing
"""

from struct import unpack

from .data import attrdict

def readNulString(stream):
    byte = stream.readUInt8()
    buf = bytes()
    while byte != 0:
        buf += bytes([byte])
        byte = stream.readUInt8()
    return buf.decode()

def parseLua(ds):
    ty = ds.readUInt8()
    if ty == 0: # num
        return ds.readFloat()
    elif ty == 1: # str
        return readNulString(ds)
    elif ty == 2: # nil
        ds.readUInt8()
        return None
    elif ty == 3: # bool
        return ds.readUInt8() != 0
    elif ty == 4: # table

        table = {}

        while ds.peek(1) != b'\x05': # 5 means table stream finished
            key = parseLua(ds)
            val = parseLua(ds)
            table[key] = val

        ds.readUInt8() # 0x5
        return table
    else:
        raise RuntimeError("Unknown lua type id: %d" % ty)

def parseHeader(ds):
    result = attrdict()
    result.ver = readNulString(ds)
    unknown = readNulString(ds)
    result.map = readNulString(ds).splitlines()[1]
    unknown2 = readNulString(ds)

    mods_size = ds.readUInt32()
    result.mods = parseLua(ds)

    scenario_size = ds.readUInt32()
    result.scenario = parseLua(ds)

    n_sources = ds.readUInt8()
    timeouts_rem = {}
    for i in range(n_sources):
        name = readNulString(ds)
        num = ds.readUInt32()

        timeouts_rem[name] = num

    result.timeouts_remaining = timeouts_rem
    result.cheats = ds.readUInt8() != 0

    n_armies = ds.readUInt8()

    cmd_sources = []

    armies = []
    for i in range(n_armies):
        data_size = ds.readUInt32()
        data = parseLua(ds)
        source_id = ds.readUInt8()

        data = dict(data)
        data.update({'source_id': source_id})

        cmd_sources.append(data)
        armies.append(data)

        if ds.peek(1) == b'\xff':
            # not sure what the hell
            ds.read(1)

    result.armies = armies
    result.random = ds.readUInt32()

    return result

class DataStream:
    "Somewhat mirrors QDataStream"
    def __init__(self, file):
        self.file = file
        self.peeked = b''

        # Collects all data read
        self._data = b''

    def peek(self, n):
        if len(self.peeked) < n:
            self.peeked += self.read(n - len(self.peeked))

        return self.peeked[:n]

    def _read(self, n):
        ret = self.file.read(n)
        self._data += ret
        if len(ret) != n:
            print('Failed to read %d, got: %s' % (n, ret))
            print(self.file.read(1))
        return ret

    def read(self, n):
        if len(self.peeked) > 0:
            if len(self.peeked) >= n:
                ret = self.peeked[:n]
                self.peeked = self.peeked[n:]
            else:
                ret = self.peeked + self._read(n - len(self.peeked))
                self.peeked = b''
            return ret
        else:
            return self._read(n)

    def readUInt8(self):
        return unpack('B', self.read(1))[0]

    def readUInt32(self):
        return unpack('<L', self.read(4))[0]

    def readFloat(self):
        return unpack('<f', self.read(4))[0]

    def data(self):
        return self._data