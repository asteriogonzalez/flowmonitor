#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""This module allows you to monitorize the file system looking for changes
and fires actions based on user handlers that are attached to the flow core.

There are two threads involved, main thread process event queue and the
second monitorize FS for changes.

Example:
        $ flowmonitor.py pelican . pelican ~/Documents/blog

TODO:
    * Be ready for huge changes (e.g. swaping git branches)

"""
import sys
import random
import os
import re
import time
import datetime
import sqlite3
import threading
import logging
from logging import info as loginfo
from collections import namedtuple
from watchdog.observers import Observer
from watchdog.events import FileMovedEvent, \
     FileSystemEventHandler, LoggingEventHandler
# from cjson import encode, decode

# from queuelib import FifoDiskQueue, PriorityQueue
# qfactory = lambda priority: FifoDiskQueue('/tmp/queue-dir-%s' % priority)

# from queuelib import FifoDiskQueue
# pq = FifoDiskQueue("queuefile")

handlers = dict()


def register_handler(klass):
    "Register a EventHandler class for command line invokation"
    assert issubclass(klass, EventHandler)
    handlers[klass.__name__] = klass
    name = klass.__name__.lower()
    name = name.split('handler')
    handlers[name[0]] = klass


Event = namedtuple('Event', ('date', 'path', 'event', 'folder',
                             'countdown', 'restart', 'path2'))


def event_key(event):
    "Provide a unique tuple that can be used as key for the event"
    return event.path, event.folder

re_split = re.compile(r'\W*(\w+)\W*(.*)$')


def split_string(line):
    "Split a string into tokens"
    tokens = []
    while line:
        m = re_split.match(line)
        if m:
            tk, line = m.groups()
            tokens.append(tk)
        else:
            break

    if line:
        tokens.append(line)
    return tokens


class DBQueue(object):
    """This class provide a Persiste Queue for events using sqlite.

    As sqlite doesn't suppor shared connection between threads,
    we implement a simple connection factory for the current thread.
    """
    def __init__(self, path='queue.db'):
        self.path = path
        self.conn__ = dict()
        self.__create_squema__()
        self.ignored__ = dict()

    def close(self):
        """Clear the processed event and close connections with database"""
        self.conn.execute("""
        DELETE FROM events
        WHERE countdown <= 0
        """)

        for conn in self.conn__.values():
            try:
                conn.commit()
                conn.close()
            except sqlite3.ProgrammingError:
                pass

    @property
    def conn(self):
        "Connection Factory per thread"
        tid = threading._get_ident()

        conn = self.conn__.get(tid)
        if conn is None:
            self.conn__[tid] = conn = sqlite3.connect(self.path)
        return conn

    def push(self, event):
        """Push an event in the queue for procesing.
        Check is a similar event has been placed to block an incoming event
        and ignore duplicated events due for example by two faster
        modifications.
        """
        ignored = self.ignored__.pop(event_key(event), None)
        if ignored:
            if event.date - ignored.date < 2:
                return

        cursor = self.conn.cursor()

        cursor.execute("""
        SELECT * FROM events
        WHERE path = ? AND event = ? AND folder = ? AND countdown > 0
        """, [event.path, event.event, event.folder])

        row = cursor.fetchone()
        if row:
            print "Ingoring Duplicate event: %s" % (event, )
        else:
            cursor.execute(
                "REPLACE INTO events VALUES (?, ?, ?, ?, ?, ?, ?)", event
            )
            self.conn.commit()

    def pop(self):
        """Pop an event from the queue."""
        cursor = self.conn.cursor()
        now = time.time()
        cursor.execute("""
        SELECT * FROM events
        WHERE date < ? AND countdown > 0
        ORDER BY date ASC LIMIT 1""", (now, ))
        raw = cursor.fetchone()
        if raw:
            event = Event(*raw)
            return event

    def restart(self, event):
        """Reschedule an event for procesing.
        Usually this events are failed events that are retry later until
        countdown is reached.
        """
        cursor = self.conn.cursor()
        now = time.time()
        cursor.execute("""
                UPDATE events SET date = ?, countdown = ?
                WHERE path = ? AND event = ? AND folder = ?
                """, [event.date + event.restart, event.countdown - 1,
                      event.path, event.event, event.folder])

        self.conn.commit()

    def finish(self, event):
        "Mark an event as finished."
        cursor = self.conn.cursor()
        cursor.execute("""
                UPDATE events SET countdown = 0
                WHERE path = ? AND event = ? AND folder = ?
                """, [event.path, event.event, event.folder])

        self.conn.commit()

    def ignore(self, event):
        "Ignore the next event of this type fror a short time"

        self.ignored__[event_key(event)] = event

    def __create_squema__(self):
        scheme = ["""CREATE TABLE IF NOT EXISTS events
     (date integer, path text, event text, folder integer,
     countdown integer, restart integer,
     path2 text,
     PRIMARY KEY (path, event, folder)
     )"""]

        conn = self.conn

        for query in scheme:
            try:
                conn.execute(query)
            except sqlite3.OperationalError, why:
                print why

        conn.commit()


class Flow(object):
    """This class implements the core for event flows and processing.

    Need a queue instance that support persistent storage and some
    handlers that accepts and process some events.

    """
    def __init__(self, queue):
        """This class needs a external queue to manage persistent events
        then creates:
        - an observer instantce that monitorize File System
        - an EventMonitor that handle earch event from Observer

        User handler are added later before running the insance.

        Args:
            queue (:obj:`instance`, ): `queue` that stores events.
        """
        self.queue = queue
        self.observer = Observer()
        self.observer.start()
        self.monitor = EventMonitor(self)
        self.handlers = list()

    def run(self):
        """Main loop of the process.
        - Poll the queue for new events.
        - Check if the event is generated under the folder tree of a handler.
        - Check is the handler is able to manage the event.

        Then add to the queue or discard the event.
        """
        # event_handler = LoggingEventHandler()
        # observer.schedule(event_handler, path, recursive=True)
        # t0 = time.time()
        loginfo('Start flow monitor')

        try:
            while True:
                time.sleep(0.2)  # non-locker style
                event = self.queue.pop()
                if event is not None:
                    try:
                        for handler in self.handlers:
                            if event.path.startswith(handler.path) and \
                               handler.match(event):
                                handler.process(event)
                    except Exception, why:
                        print why
                        self.queue.restart(event)
                    else:
                        self.queue.finish(event)

        except KeyboardInterrupt:
            loginfo('User Keyboard interrupt')

        self.close()

    def close(self):
        "Close all Flow activities and close the event queue"
        self.observer.stop()
        self.observer.join()
        self.queue.close()

    def add(self, handler):
        """Add a handler that manage some events under a directory.
        """
        self.observer.schedule(self.monitor, handler.path, recursive=True)
        self.handlers.append(handler)
        handler.flow = self
        loginfo('Add handler: %s on %s' % (handler.__class__.__name__,
                                           handler.path))


class EventMonitor(FileSystemEventHandler):
    """Logs all the events captured."""

    def __init__(self, flow):
        self.flow = flow

    def on_any_event(self, event):
        now = time.time()
        if event.__class__ == FileMovedEvent:
            ev, path2, path, folder = event.key
        else:
            ev, path, folder = event.key
            path2 = ''

        # ev = self.tran[ev]
        path = os.path.abspath(path)
        ev = Event(now, path, ev, folder, 5, 10, path2)
        for handler in self.flow.handlers:
            if ev.path.startswith(handler.path):
                if handler.match(ev):
                    flow.queue.push(ev)


class EventHandler(object):
    "Base class for all EventHandlers"
    def __init__(self, path):
        """Base class for any event handler.

        Args:
            path (:obj:`string`, ): The `path` for monitoring into.
        """
        path = os.path.expanduser(path)
        self.path = os.path.abspath(path)
        self.flow = None

    def match(self, event):
        "Determine if we can handle this event"
        return True

    def process(self, event):
        """Search for a `on_xxxx` method that will handle the event
        in the subclass instance.
        """
        func_name = 'on_%s' % event.event
        func = getattr(self, func_name, None)
        if func:
            func(event)


class PelicanHandler(EventHandler):
    """This class manage pelican markdown files following these rules:

    - When a file is modified, the file header is modified with
    the time rounded by 5 min.

    - When a file is created, modified or moved and its size is zero
    then search the best template that suits the name and location of the file

    """
    _re_headers = re.compile(r'(?P<name>\w*)\s*:\s*(?P<value>.*)\s*\n$',
                             re.MULTILINE | re.DOTALL | re.IGNORECASE)
    _re_templates = re.compile(r'.*template|templates.*', re.I | re.DOTALL)

    def __init__(self, path):
        EventHandler.__init__(self, path)
        # self.templates = dict()

    def match(self, event):
        "Determine if we can handle this event"
        return event.path.endswith('.md')

    def on_created(self, event):
        "Fired when a new file is created"
        info = os.stat(event.path)
        if info.st_size <= 0:
            loginfo('Fill %s from template' % event.path)

            lines = self.find_template(event)
            self.write_file(event, lines)

    def on_modified(self, event):
        "Fired when a file is modified somehow"
        info = os.stat(event.path)
        if info.st_size <= 0:
            return self.on_created(event)

        replace = dict()
        t = (event.date // 300) * 300
        replace['Modified'] = datetime.datetime.fromtimestamp(t) \
            .strftime('%Y-%m-%d %H:%M')

        self.update_headers(event, replace)

    def on_moved(self, event):
        "Fired when a file is renamed of moved"
        return self.on_modified(event)

    def on_deleted(self, event):
        "Fired when a file is deleted"
        pass

    def update_headers(self, event, replace):
        "Replace some file headers and write down the new content"
        lines = []
        with file(event.path, 'rt') as f:
            for line in f.readlines():
                m = self._re_headers.match(line)
                if m:
                    value = replace.get(m.groupdict().get('name'))
                    if value is not None:
                        line = self._re_headers.sub(
                            r'\g<name>: %s\n' % value, line)
                lines.append(line)

            self.write_file(event, lines)

    def write_file(self, event, lines):
        """Write a text file having all the lines.
        Request to ignore the next modified event from this file
        breaking a infinite self-update loop.
        """
        self.flow.queue.ignore(event)

        with file(event.path, 'wt') as f:
            f.writelines(lines)

    def find_template(self, event):
        """Find a template"""
        newname = event.path.split(self.path)[-1].lower()
        ext = os.path.splitext(event.path)[-1]
        new_tokens = set(split_string(newname))
        best_match = 0
        lines = ['this is an empty article!']

        for root, folders, files in os.walk(self.path):
            if self._re_templates.match(root):
                for name in files:
                    namelower = name.lower()
                    if os.path.splitext(namelower)[-1] == ext:
                        tokens = split_string(namelower)
                        match = new_tokens.intersection(tokens)
                        if match > best_match:
                            best_match = match
                            lines = file(os.path.join(root, name),
                                         'rt').readlines()

        return lines


class PyTestHandler(EventHandler):
    """A simple Handler dummy class intended to launch py.tests
    when a python file is modified.
    """

    def match(self, event):
        "Determine if we can handle this event"
        return event.path.endswith('.py')


def config(flow):
    "A simple example config"

    args = list(sys.argv[1:])
    while args:
        name = args.pop(0)
        klass = handlers.get(name)
        if klass is None:
            raise RuntimeError('Unknown handler %s' % name)
        n = klass.__init__.im_func.func_code.co_argcount - 1
        init_args, args = args[:n], args[n:]
        handler = klass(*init_args)
        flow.add(handler)

# register this module Handlers
register_handler(PelicanHandler)
register_handler(PyTestHandler)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.basicConfig(level=logging.INFO)

    queue = DBQueue()
    flow = Flow(queue)
    config(flow)
    flow.run()
