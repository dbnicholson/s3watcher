import asyncio

from .exceptions import S3WatcherError
from .log import logger
from .message import MessageType, write_message, read_message


class S3WatcherClient(object):
    """S3Watcher client

    Opens a connection to an S3Watcher server and provides methods for
    making requests to it.
    """
    def __init__(self, addr='127.0.0.1', port=7979):
        self.addr = addr
        self.port = port
        self.reader = None
        self.writer = None

    async def connect(self):
        """Initiate a connection to the server"""
        logger.info('Opening connection to %s %d', self.addr, self.port)
        self.reader, self.writer = await asyncio.open_connection(self.addr,
                                                                 self.port)

    def _ensure_connection(self):
        if self.reader is None or self.writer is None:
            raise S3WatcherError('Not connected to server')

    async def _request(self, message):
        self._ensure_connection()
        await write_message(self.writer, message)
        msgtype, response = await read_message(self.reader)
        if msgtype == MessageType.ERROR:
            raise S3WatcherError(response)
        return response

    async def objects(self):
        """Make an objects request to the server"""
        return await self._request((MessageType['OBJECTS'],))
