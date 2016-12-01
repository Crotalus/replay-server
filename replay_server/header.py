"""
GPG Replay Header parsing
"""
import logging
from struct import unpack
from .data import attrdict

log = logging.getLogger(__name__)

async def readNulString(stream):
    byte = await stream.readUInt8()
    buf = bytes()
    while byte != 0:
        buf += bytes([byte])
        byte = await stream.readUInt8()
    return buf.decode()

async def parseLua(ds):
    ty = await ds.readUInt8()
    if ty == 0:  # num
        return await ds.readFloat()
    elif ty == 1:  # str
        return await readNulString(ds)
    elif ty == 2:  # nil
        await ds.readUInt8()
        return None
    elif ty == 3:  # bool
        return await ds.readUInt8() != 0
    elif ty == 4:  # table

        table = {}

        while await ds.peek(1) != b'\x05':  # 5 means table stream finished
            key = await parseLua(ds)
            val = await parseLua(ds)
            table[key] = val

        await ds.readUInt8()  # 0x5
        return table
    else:
        raise RuntimeError("Unknown lua type id: %d" % ty)

async def parseHeader(ds):
    result = attrdict()
    result.ver = await readNulString(ds)
    result.unknown = await readNulString(ds)
    result.map = (await readNulString(ds)).splitlines()[1]
    result.unknown2 = await readNulString(ds)

    result.mods_size = await ds.readUInt32()
    result.mods = await parseLua(ds)

    result.scenario_size = await ds.readUInt32()
    result.scenario = await parseLua(ds)

    n_sources = await ds.readUInt8()
    timeouts_rem = {}

    for _ in range(n_sources):
        name = await readNulString(ds)
        num = await ds.readUInt32()

        timeouts_rem[name] = num

    result.timeouts_remaining = timeouts_rem
    result.cheats = await ds.readUInt8() != 0

    n_armies = await ds.readUInt8()

    cmd_sources = []

    armies = []
    for _ in range(n_armies):
        # data_size
        _ = await ds.readUInt32()
        data = await parseLua(ds)
        source_id = await ds.readUInt8()

        data = dict(data)
        data.update({'source_id': source_id})

        cmd_sources.append(data)
        armies.append(data)

        if await ds.peek(1) == b'\xff':
            # not sure what the hell
            await ds.read(1)

    result.armies = armies
    result.random = await ds.readUInt32()

    return result


class DataStream:
    "Somewhat mirrors QDataStream"

    def __init__(self, file):
        self.file = file
        self.peeked = b''

        # Collects all data read
        self._data = b''

    async def peek(self, n):
        if len(self.peeked) < n:
            self.peeked += await self.read(n - len(self.peeked))

        return self.peeked[:n]

    async def _read(self, n):
        ret = b''
        n_read = 0
        while n_read < n:
            new = await self.file.read(n-n_read)
            if not new:
                log.error("Failed to read %d of %d bytes from stream", n-n_read, n)
                break

            ret += new
            n_read = len(ret)

        self._data += ret

        return ret

    async def read(self, n):
        n_peeked = len(self.peeked)
        if n_peeked > 0:
            if n_peeked >= n:
                ret = self.peeked[:n]
                self.peeked = self.peeked[n:]
            else:
                ret = self.peeked + await self._read(n - n_peeked)
                self.peeked = b''
            return ret
        else:
            return await self._read(n)

    async def readUInt8(self):
        return unpack('B', await self.read(1))[0]

    async def readUInt32(self):
        return unpack('<L', await self.read(4))[0]

    async def readFloat(self):
        return unpack('<f', await self.read(4))[0]

    def data(self):
        return self._data
