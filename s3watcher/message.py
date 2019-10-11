from enum import IntEnum
import msgpack

from .log import logger
from .exceptions import S3WatcherError


# The protocol is pretty simple. The message consists of a header and
# body. The body is the message payload in MessagePack format. The
# header begins with a single byte version number and then 4 bytes
# containing the payload size in big endian.
#
# The format of the body is a tuple of (msgtype, parameters). msgtype is
# integer enumeration of the message type. Parameters is an optional
# dictionary. The keys and values in the dictionary are dependent on the
# message type.
MESSAGE_VERSION = 0
MESSAGE_HEADER_SIZE = 5


class MessageType(IntEnum):
    ERROR = 0
    OBJECTS = 1


async def write_message(writer, data):
    body = msgpack.packb(data, use_bin_type=True)
    size = len(body)
    header = MESSAGE_VERSION.to_bytes(1, 'big') + size.to_bytes(4, 'big')
    writer.write(header + body)
    await writer.drain()


async def write_error(writer, error):
    await write_message(writer, (MessageType.ERROR, error))


async def read_message(reader):
    header = await reader.readexactly(MESSAGE_HEADER_SIZE)
    version = header[0]
    if version != MESSAGE_VERSION:
        raise S3WatcherError('Protocol version {} not supported'
                             .format(version))
    size = int.from_bytes(header[1:], 'big')

    unpacker = msgpack.Unpacker(use_list=False, raw=False)
    while size > 0:
        buf = await reader.read(size)
        size -= len(buf)
        unpacker.feed(buf)

    return unpacker.unpack()


async def validate_message(writer, data):
    if not isinstance(data, tuple):
        logger.error('message is not tuple')
        await write_error(writer, 'message is not tuple')
        return False

    if not isinstance(data[0], int):
        logger.error('message member 0 is not an integer')
        await write_error(writer, 'message member 0 is not an integer')
        return False

    try:
        msgtype = MessageType(data[0])
        logger.debug('valid message type %s', msgtype)
    except ValueError:
        logger.error('unknown message type %d', data[0])
        await write_error(writer, 'unknown message type %d' % data[0])
        return False

    return True
