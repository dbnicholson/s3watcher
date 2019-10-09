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

import boto3
from collections import namedtuple
from gi.repository import GLib, Gio
import json
import logging
from operator import attrgetter
import os
import signal
import sys
import threading
from urllib.parse import unquote_plus


logger = logging.getLogger(os.path.basename(__file__))


class S3WatcherError(Exception):
    """Errors for S3Watcher class"""
    pass


class S3Watcher(object):
    """Watch images S3 bucket

    Cache the objects in the images S3 bucket and make the listing
    available over DBus.
    """
    BUS = 'com.endlessm.S3Listing'
    PATH = '/com/endlessm/S3Listing'
    INTERFACE = 'com.endlessm.S3Listing'
    INTROSPECTION_XML = \
        """\
        <node>
          <interface name="{interface}">
            <method name="Objects">
              <arg name="objects" direction="out" type="a(stt)"/>
            </method>
          </interface>
        </node>
        """.format(interface=INTERFACE)

    # The SQS queue is not guaranteed to deliver all events, and S3
    # doesn't deliver events for bucket lifecycle events, so refresh the
    # cached objects periodically.
    REFRESH_INTERVAL = 10 * 60

    # The SQS queue is checked each time the Objects method is called,
    # but this is done with a "short poll" and may not receive all
    # events. Periodically perform a long poll to ensure all SQS events
    # are received. This also keeps the SQS queue from growing large.
    FLUSH_INTERVAL = 30

    def __init__(self, bucket, queue_url=None, region=None):
        if region is None:
            region = 'us-east-1'
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

        # DBus registration ID
        self.bus_id = None

        # Object listing cache and associated locks
        self.objects = {}
        self.objects_lock = threading.Lock()
        self.queue_lock = threading.Lock()

        logger.info('Starting initial enumeration')
        self._refresh_all_objects()
        self._flush_queue()
        logger.info('Finished initial enumeration')

        # Setup periodic tasks to refresh the object cache and process
        # events
        self._refresh_source_id = None
        self._flush_source_id = None
        self.setup_sources()

    def __del__(self):
        # Drop bus ownership and remove timeout sources when deleted
        self.remove_dbus()
        self.remove_sources()

    def _refresh_all_objects(self):
        """Fully refresh object cache

        Get a full listing from S3 and replace the object cache with it.
        """
        logger.debug('Refreshing all objects')
        tmpobjects = {}
        for obj in self.bucket.objects.all():
            self._add_object(tmpobjects, obj)
        with self.objects_lock:
            self.objects = tmpobjects

    # Event information created from SQS S3 records
    ObjectEvent = namedtuple('ObjectEvent',
                             ['name', 'key', 'created', 'sequence'])

    def _flush_queue(self, wait_seconds=3):
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
        with self.queue_lock:
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
            self._process_events(sorted_events)

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

    def _process_events(self, events):
        """Add or remove objects from cache based on events"""
        with self.objects_lock:
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

    def _method_call(self, connection, sender, object_path, interface_name,
                     method_name, parameters, invocation):
        """DBus method call handler"""
        if object_path != self.PATH:
            raise Exception('Unrecognized object path "{}"'.format(object_path))

        if method_name == 'Objects':
            # Get any outstanding events with a short poll
            self._flush_queue(wait_seconds=0)
            listing = []
            for key, value in sorted(self.objects.items()):
                listing.append((key, value[0], value[1]))
            ret = GLib.Variant.new_tuple(
                GLib.Variant('a(stt)', listing)
            )
            invocation.return_value(ret)
        else:
            raise Exception('Unrecognized method "{}"'.format(method_name))

    def setup_dbus(self, bus_type=Gio.BusType.SYSTEM,
                   name_lost_handler=None):
        """Setup DBus connection"""
        logger.info('Claiming bus %s', self.BUS)
        self.bus_id = Gio.bus_own_name(bus_type,
                                       self.BUS,
                                       Gio.BusNameOwnerFlags.NONE,
                                       self._register_object,
                                       None,
                                       name_lost_handler)

    def remove_dbus(self):
        """Drop DBus bus name"""
        if self.bus_id is not None:
            logger.info('Dropping bus %s', self.BUS)
            Gio.bus_unown_name(self.bus_id)

    def _register_object(self, connection, name):
        """Register object for DBus path"""
        logger.info('Registering object for %s', self.PATH)
        node_info = Gio.DBusNodeInfo.new_for_xml(self.INTROSPECTION_XML)
        Gio.DBusConnection.register_object(connection,
                                           self.PATH,
                                           node_info.interfaces[0],
                                           self._method_call,
                                           None,
                                           None)

    def setup_sources(self):
        """Setup cache handling tasks"""
        self._setup_refresh_source()
        self._setup_flush_source()

    def remove_sources(self):
        """Remove cache handling tasks"""
        self._remove_refresh_soure()
        self._remove_flush_source()

    def _setup_refresh_source(self):
        logger.debug('Enabling refresh source')
        self._refresh_source_id = GLib.timeout_add_seconds(
            self.REFRESH_INTERVAL, self._handle_refresh_timeout)

    def _handle_refresh_timeout(self):
        logger.debug('Handling refresh timeout')
        self._refresh_all_objects()
        self._setup_refresh_source()
        return False

    def _remove_refresh_source(self):
        if self._refresh_source_id is not None:
            logger.debug('Removing refresh source')
            GLib.source_remove(self._refresh_source_id)
            self._refresh_source_id = None

    def _setup_flush_source(self):
        if self.queue is not None:
            logger.debug('Enabling flush source')
            self._flush_source_id = GLib.timeout_add_seconds(
                self.FLUSH_INTERVAL, self._handle_flush_timeout)
        else:
            self._flush_source_id = None

    def _handle_flush_timeout(self):
        logger.debug('Handling flush timeout')
        self._flush_queue()
        self._setup_flush_source()
        return False

    def _remove_flush_source(self):
        if self._flush_source_id is not None:
            logger.debug('Removing flush source')
            GLib.source_remove(self._flush_source_id)
            self._flush_source_id = None


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

    server = S3Watcher(args.bucket, args.queue_url, args.region)
    loop = GLib.MainLoop()

    # Gracefully exit when killed
    def sigterm_handler(signal, frame):
        logger.info('Received SIGTERM, exiting')
        loop.quit()

    signal.signal(signal.SIGTERM, sigterm_handler)

    # Fail if the bus name is lost
    def name_lost_handler(connection, name):
        logger.error('Bus name %s lost, exiting', name)
        sys.exit(1)

    # Initialize DBus
    bus_type = Gio.BusType.SESSION if args.session else Gio.BusType.SYSTEM
    server.setup_dbus(bus_type, name_lost_handler)

    try:
        loop.run()
    except KeyboardInterrupt:
        logger.info('Received keyboard interrupt, exiting')


if __name__ == '__main__':
    main()
