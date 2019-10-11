#!/usr/bin/python3

# s3watcher - Watch S3 bucket contents with RPC
#
# Copyright (C) 2019  Dan Nicholson <nicholson@endlessm.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import asyncio
import boto3
from collections import namedtuple
from enum import IntEnum
import json
import logging
import msgpack
from operator import attrgetter
import os
import signal
import sys
from urllib.parse import unquote_plus


logger = logging.getLogger(os.path.basename(__file__))


class S3WatcherError(Exception):
    """Errors for S3Watcher class"""
    pass


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
    except ValueError:
        logger.error('unknown message type %d', data[0])
        await write_error(writer, 'unknown message type %d' % data[0])
        return False

    return True


class S3Watcher(object):
    """Watch images S3 bucket

    Cache the objects in the images S3 bucket and make the listing
    available over RPC.
    """
    # The SQS queue is not guaranteed to deliver all events, and S3
    # doesn't deliver events for bucket lifecycle events, so refresh the
    # cached objects periodically.
    REFRESH_INTERVAL = 10 * 60

    # The SQS queue is checked each time the Objects method is called,
    # but this is done with a "short poll" and may not receive all
    # events. Periodically perform a long poll to ensure all SQS events
    # are received. This also keeps the SQS queue from growing large.
    FLUSH_INTERVAL = 30

    def __init__(self, bucket, queue_url=None, region='us-east-1',
                 addr='127.0.0.1', port=7979):
        self.addr = addr
        self.port = port

        session = boto3.session.Session(region_name=region)
        self.s3 = session.resource('s3')
        self.bucket = self.s3.Bucket(bucket)

        if queue_url:
            self.sqs = session.resource('sqs')
            self.queue = self.sqs.Queue(queue_url)
            # Purge the queue since we're about to re-enumerate the
            # whole bucket and don't need to bother reading old records
            try:
                self.queue.purge()
            except self.sqs.meta.client.exceptions.PurgeQueueInProgress:
                pass
        else:
            self.sqs = None
            self.queue = None

        # Object listing cache and associated locks
        self.objects = {}
        self.objects_lock = asyncio.Lock()
        self.queue_lock = asyncio.Lock()

        # Server task
        self.server = None

        # Setup periodic tasks to refresh the object cache and process
        # events
        self._refresh_task = None
        self._flush_task = None

    def __del__(self):
        # Remove tasks when deleted
        self.remove_tasks()

    async def _refresh_all_objects(self):
        """Fully refresh object cache

        Get a full listing from S3 and replace the object cache with it.
        """
        logger.debug('Refreshing all objects')
        tmpobjects = {}
        for obj in self.bucket.objects.all():
            self._add_object(tmpobjects, obj)
        async with self.objects_lock:
            self.objects = tmpobjects

    # Event information created from SQS S3 records
    ObjectEvent = namedtuple('ObjectEvent',
                             ['name', 'key', 'created', 'sequence'])

    async def _flush_queue(self, wait_seconds=3):
        """Receive and process all messages in SQS queue

        By default, a "long poll" is done to try to ensure that all
        messages are received. A "short poll" can be done instead by
        specifying wait_seconds as 0.

        See
        https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
        for details.
        """
        if self.queue is None:
            return

        # Lock the queue in case there's already another reader
        async with self.queue_lock:
            # SQS can deliver the messages out of order, so get them all
            # and sort them below
            all_messages = []
            while True:
                messages = self.queue.receive_messages(
                    MaxNumberOfMessages=10, WaitTimeSeconds=wait_seconds)
                num_messages = len(messages)
                logger.debug('Received %i message%s', num_messages,
                             '' if num_messages == 1 else 's')
                if num_messages == 0:
                    break
                all_messages.extend(messages)

            # Get a list of relevant ObjectEvent
            events = []
            for msg in all_messages:
                logger.debug('Reading message %s', msg.message_id)
                body = json.loads(msg.body)
                for record in body.get('Records', []):
                    event = self._create_event(record)
                    if event is not None:
                        events.append(event)

            # Sort the events by sequence and process them
            sorted_events = sorted(events, key=attrgetter('sequence'))
            await self._process_events(sorted_events)

            # Delete all the messages since they've been processed
            for msg in all_messages:
                logger.debug('Deleting message %s', msg.message_id)
                msg.delete()

    def _create_event(self, record):
        """Convert S3 SQS record to ObjectEvent

        See
        https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
        for record details.
        """
        event_source = record.get('eventSource')
        if event_source != 'aws:s3':
            logger.debug('Ignoring record from source %s', event_source)
            return None

        # We require event major version 2. Fail otherwise.
        event_major_version = record['eventVersion'].split('.')[0]
        if event_major_version != '2':
            logger.error('Ignoring unsupported event version %s',
                         record['eventVersion'])
            return None

        bucket = record['s3']['bucket']['name']
        if bucket != self.bucket.name:
            logger.debug('Ignoring record for bucket %s', bucket)
            return None

        event_name = record['eventName']
        if event_name.startswith('ObjectCreated:'):
            created = True
        elif event_name.startswith('ObjectRemoved:'):
            created = False
        else:
            logger.debug('Ignoring non-object event %s', event_name)
            return None

        # The object key is URL encoded as for an HTML form
        key = unquote_plus(record['s3']['object']['key'])

        # The sequencer value is a hex string
        sequence = int(record['s3']['object']['sequencer'], base=16)

        return self.ObjectEvent(event_name, key, created, sequence)

    async def _process_events(self, events):
        """Add or remove objects from cache based on events"""
        async with self.objects_lock:
            for event in events:
                logger.info('Received %s event for "%s"', event.name,
                            event.key)
                if event.created:
                    # Get the summary of this object but be prepared for
                    # it to be deleted
                    summary = self.s3.ObjectSummary(self.bucket.name,
                                                    event.key)
                    try:
                        summary.load()
                    except self.s3.meta.client.exceptions.NoSuchKey:
                        logger.debug('Ignoring deleted key "%s"',
                                     event.key)
                        self._delete_object(self.objects, event.key)
                        continue
                    self._add_object(self.objects, summary)
                else:
                    self._delete_object(self.objects, event.key)

    @staticmethod
    def _add_object(objects, summary):
        """Add object to cache"""
        last_modified = int(summary.last_modified.timestamp())
        logger.debug('Adding object "%s", size %i, modified %i',
                     summary.key, summary.size, last_modified)
        objects[summary.key] = (summary.size, last_modified)

    @staticmethod
    def _delete_object(objects, key):
        """Remove object from cache"""
        logger.debug('Removing object "%s"', key)
        objects.pop(key, None)

    async def _handle_objects(self, writer, data):
        # Get any outstanding events with a short poll
        await self._flush_queue(wait_seconds=0)
        listing = []
        for key, value in sorted(self.objects.items()):
            listing.append((key, value[0], value[1]))
        await write_message(writer, ('objects', listing))

    async def handle_connection(self, reader, writer):
        data = await read_message(reader)
        if await validate_message(writer, data):
            msgtype = data[0]
            logger.debug('received message type %s', msgtype)
            if msgtype == MessageType.OBJECTS:
                await self._handle_objects(writer, data)
            else:
                logger.error('unknown message type %s', method)
                await write_error(writer,
                                  'unknown message type {}'.format(method))
        writer.close()

    def setup_tasks(self):
        """Setup cache handling tasks"""
        self._setup_refresh_task()
        self._setup_flush_task()

    def remove_tasks(self):
        """Remove cache handling tasks"""
        self._remove_refresh_task()
        self._remove_flush_task()

    def _setup_refresh_task(self):
        logger.debug('Enabling refresh task')
        self._refresh_task = asyncio.create_task(self._handle_refresh_task())

    async def _handle_refresh_task(self):
        await asyncio.sleep(self.REFRESH_INTERVAL)
        logger.debug('Handling refresh task')
        await self._refresh_all_objects()
        self._setup_refresh_task()

    def _remove_refresh_task(self):
        if self._refresh_task is not None:
            logger.debug('Removing refresh task')
            self._refresh_task.cancel()
            self._refresh_task = None

    def _setup_flush_task(self):
        if self.queue is None:
            self._flush_task = None
            return

        logger.debug('Enabling flush task')
        self._flush_task = asyncio.create_task(self._handle_flush_task())

    async def _handle_flush_task(self):
        await asyncio.sleep(FLUSH_INTERVAL)
        logger.debug('Handling flush task')
        await self._flush_queue()
        self._setup_flush_task()

    def _remove_flush_task(self):
        if self._flush_task is not None:
            logger.debug('Removing flush task')
            self._flush_task.cancel()
            self._flush_task = None

    async def run(self):
        logger.info('Starting initial enumeration')
        await self._refresh_all_objects()
        await self._flush_queue()
        logger.info('Finished initial enumeration')

        self.setup_tasks()

        server = await asyncio.start_server(self.handle_connection,
                                            self.addr,
                                            self.port)
        async with server:
            await server.serve_forever()


def main():
    from argparse import ArgumentParser

    aparser = ArgumentParser(
        description='Image S3 bucket watch service')
    aparser.add_argument('bucket', metavar='BUCKET',
                         help='S3 bucket name')
    aparser.add_argument('queue_url', metavar='QUEUE', nargs='?',
                         help='SQS queue URL')
    aparser.add_argument('--region', help='S3 region')
    aparser.add_argument('--session', action='store_true',
                         help='use DBus session bus')
    aparser.add_argument('--debug', action='store_true',
                         help='enable debug messages')
    args = aparser.parse_args()

    logging.basicConfig(level=logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    watcher = S3Watcher(args.bucket, args.queue_url, args.region)
    asyncio.run(watcher.run())


if __name__ == '__main__':
    main()
