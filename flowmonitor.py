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
    * Manage .rst files in PelicanHandler
    * Remove remote files that doesn't match the rules anymore

"""
import sys
import random
import os
import re
import time
import types
import datetime
import sqlite3
import threading
import logging
from logging import info as loginfo, warning as logwarning
import subprocess
import hashlib
from getpass import getuser
from collections import namedtuple
from multiprocessing import Process
from watchdog.observers import Observer
from watchdog.events import FileSystemMovedEvent, \
     FileSystemEventHandler, LoggingEventHandler
import yaml

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


def split_liststr(data):
    "Split a string into CSV list"
    if isinstance(data, types.StringTypes):
        info = list()
        for ext in data.split(','):
            info.append(ext.strip())
        return info
    return data


def are_equals(source, target):
    """
    Asume both files exists
    """
    info_source = os.stat(source)
    info_target = os.stat(target)

    if info_source.st_size != info_target.st_size:
        return False

    # check sha1
    m_source = hashlib.sha1()
    with file(source, 'r') as f:
        m_source.update(f.read())
    m_target = hashlib.sha1()
    with file(target, 'r') as f:
        m_target.update(f.read())

    return m_source.digest() == m_target.digest()

primitives = (int, str, bool, unicode, list, dict, float)


class Persistent(object):
    """Base class for persistence"""

    def __getstate__(self):
        cfg = dict()
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            if not (isinstance(value, primitives) or
                    issubclass(value.__class__, primitives)):
                continue

            cfg[key] = value
        return cfg

    def __setstate__(self, cfg):
        self.__dict__.update(cfg)


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
        self.clean()

        for conn in self.conn__.values():
            try:
                conn.commit()
                conn.close()
            except sqlite3.ProgrammingError:
                pass

    def clean(self):
        "Clean the processed event queue"
        self.conn.execute("""
                DELETE FROM events
                WHERE countdown <= 0
                """)

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

    def get_last_event(self, since=None):
        """Get the older event in queue"""
        cursor = self.conn.cursor()
        since = since or time.time()
        cursor.execute("""
        SELECT * FROM events
        WHERE date < ? AND countdown <= 0
        ORDER BY date DESC LIMIT 1""", (since, ))
        raw = cursor.fetchone()
        if raw:
            event = Event(*raw)
            return event

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


class Flow(Persistent):
    """This class implements the core for event flows and processing.

    Need a queue instance that support persistent storage and some
    handlers that accepts and process some events.

    """
    CFG_NAME = 'flow.yaml'

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
        self.eventpolling = 0.2
        self.idlecycles = 50
        self.configfile = ''

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
        loginfo('Saving configuration file')
        self.save_config()

        loginfo('Start flow monitor')

        try:
            while True:
                for counter in xrange(self.idlecycles):
                    time.sleep(self.eventpolling)  # non-locker style
                    self.next()
                self.idle()

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

    def next(self):
        "Try to execute the next event in queue"
        event = self.queue.pop()
        if event is None:
            pass
        else:
            try:
                for handler in self.handlers:
                    if event.path.startswith(handler.path) and \
                       handler.match(event):
                        handler.process(event)
            except Exception, why:
                logwarning(why)
                self.queue.restart(event)
            else:
                self.queue.finish(event)

    def idle(self):
        "Performs tasks that suit in idle loops from time to time"
        now = time.time()
        event = self.queue.get_last_event(now - 10)
        if event:
            print "Do something idle ..."
            for handler in self.handlers:
                handler.on_idle()

    def save_config(self, configfile=None):
        """Save internal config to a file"""
        self.configfile = configfile or self.CFG_NAME

        cfg = self.__getstate__()

        handlers = cfg['handlers'] = list()

        for handler in self.handlers:
            handlers.append([handler.__class__.__name__,
                             handler.__getstate__()])

        with file(self.configfile, 'w') as f:
            f.write(yaml.safe_dump(cfg))

    def load_config(self, configfile=None):
        """Load config from file and create the needed handlers"""
        self.configfile = configfile or self.CFG_NAME

        if os.path.exists(self.configfile):
            with file(self.configfile, 'r') as f:
                cfg = yaml.safe_load(f.read())

            if cfg:
                for klass, state in cfg.pop('handlers'):
                    print klass, state
                    instance = handlers[klass](**state)
                    self.add(instance)

                self.__setstate__(cfg)

                return True

    def config(self, configfile=None):
        "A simple example config"

        args = list(sys.argv[1:])
        if len(args) <= 0:
            # try to load last configuration used
            if self.load_config(configfile):
                return
        elif not args:
            raise RuntimeError(
                'You need to specify al least one plugin handler')

        # use command line settings
        while args:
            name = args.pop(0)
            klass = handlers.get(name)
            if klass is None:
                raise RuntimeError('Unknown handler %s' % name)
            n = klass.__init__.im_func.func_code.co_argcount - 1
            init_args, args = args[:n], args[n:]
            handler = klass(*init_args)
            self.add(handler)


class EventMonitor(FileSystemEventHandler):
    """Logs all the events captured."""

    def __init__(self, flow):
        self.flow = flow

    def on_any_event(self, event):
        now = time.time()
        if isinstance(event, FileSystemMovedEvent):
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


class EventHandler(Persistent):
    "Base class for all EventHandlers"
    def __init__(self, path, extensions):
        """Base class for any event handler.

        Args:
            path (:obj:`string`, ): The `path` for monitoring into.
        """
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            raise OSError("%s path does not exits" % path)

        self.flow = None

        self.path = os.path.abspath(path)
        self.extensions = split_liststr(extensions)

    def match(self, event):
        "Determine if we can handle this event"
        return self._match(event.path)

    def _match(self, filename):
        ext = os.path.splitext(filename)[-1]
        return ext in self.extensions

    def process(self, event):
        """Search for a `on_xxxx` method that will handle the event
        in the subclass instance.
        """
        func_name = 'on_%s' % event.event
        func = getattr(self, func_name, None)
        if func:
            func(event)

    def make(self, event):
        """Execute a external process that 'makes' the goal
        of the handler.

        The event is passed as a reference.
        """
        return True

    def on_idle(self):
        "Performs idle tasks"


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

    def __init__(self, path, extensions='.md'):
        EventHandler.__init__(self, path, extensions)

    def on_created(self, event):
        "Fired when a new file is created"
        info = os.stat(event.path)
        if info.st_size <= 0:
            loginfo('Fill %s from template' % event.path)

            lines = self.find_template(event)
            if lines:
                self.update_contents(event, lines)

    def on_modified(self, event):
        "Fired when a file is modified somehow"
        info = os.stat(event.path)
        if info.st_size <= 0:
            return self.on_created(event)

        with file(event.path, 'rt') as f:
            lines = f.readlines()
            self.update_contents(event, lines)

    def on_moved(self, event):
        "Fired when a file is renamed of moved"
        return self.on_modified(event)

    def on_deleted(self, event):
        "Fired when a file is deleted"
        pass

    def update_contents(self, event, lines):
        """Update headers and variables of each file line"""
        headers = dict()
        t = (event.date // 300) * 300
        headers['Modified'] = datetime.datetime.fromtimestamp(t) \
            .strftime('%Y-%m-%d %H:%M')
        self.update_headers(lines, headers)

        if not self._re_templates.match(event.path):
            replace = dict()
            replace[r'\$random\$'] = (random.randint, 0, 1000)
            replace[r'\$date\$'] = headers['Modified']
            replace[r'\$user\$'] = getuser()
            self.update_replacements(lines, replace)

        self.write_file(event, lines)
        self.make(event)

    def update_headers(self, lines, replace):
        "Replace some file headers and write down the new content"
        for i, line in enumerate(lines):
            m = self._re_headers.match(line)
            if m:
                value = replace.get(m.groupdict().get('name'))
                if value is not None:
                    line = self._re_headers.sub(
                        r'\g<name>: %s\n' % value, line)
                    lines[i] = line

    def update_replacements(self, lines, replace):
        "Replace some file headers and write down the new content"
        for i, line in enumerate(lines):
            for regex, value in replace.items():
                m = re.search(regex, line)
                if m:
                    if isinstance(value, types.TupleType):
                        value = value[0](*value[1:])
                    if not isinstance(value, types.StringTypes):
                        value = str(value)
                    line = re.sub(regex, value, line)
                    lines[i] = line

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

    def make(self, event):
        """Update the output contents for pelican.

        The event is passed as a reference.
        """
        args = ['pelican']
        proc = subprocess.Popen(args, cwd=self.path)

        proc.wait()
        return proc.returncode


class SyncHandler(EventHandler):
    """This class manage two or more trees to propagate changes
    across them.

    There have two methods:

    - Fast: uses the database queue to select only then know archives
            but may lack some files if flow was down when changes happened.

    - Deep: explore all file system tree

    The matching rules can be describe as a sequences of regular expressions
    that any file matching any of them, will be synchronized.
    """

    def __init__(self, path, extensions='.md', remotes='/tmp/remote',
                 rules=r'Tags:\s*.*\W+(foo)\W+.*', location='ai',
                 delete_missing=False):
        EventHandler.__init__(self, path, extensions)
        self.rules = split_liststr(rules)
        self.remotes = split_liststr(remotes)
        self.location = split_liststr(location)
        self.delete_missing = delete_missing

    # def add_rule(self, regexp):
        # self.rules.append(regexp)
        # self._rules.append(re.compile(regexp))

    def on_idle(self):
        "Performs syncing tasks"

        print "Idle on syncing"
        # p = Process(target=self.sync)
        # p.start()
        # p.join()
        self.sync()  # for debugging

    def sync(self):
        """Sync files that match criteria and remove any remote
        file that must not be there."""
        loginfo("Start Sync")
        # sync files to add
        for root, folders, files in os.walk(self.path):
            for name in files:
                filename = os.path.join(root, name)
                if self._match(filename):
                    if self.match_content(filename):
                        self.sync_one_file(filename)

        # sync files that must be deleted
        for remote in self.remotes:
            self.delete_not_matched(remote)

    def match_content(self, filename):
        """Analize file content that match any of the given rules."""
        intersection = set(self.location).intersection(
            split_string(os.path.dirname(filename)))
        if intersection:
            return intersection

        if not os.path.exists(filename):
            return

        with file(filename, 'rt') as f:
            for line in f.readlines():
                for regexp in self.rules:
                    m = re.match(regexp, line)
                    if m:
                        return m

    def sync_one_file(self, filename):
        """Try to sync a file into all remote folders."""
        relative_path = filename.split(self.path)[-1]
        for remote in self.remotes:
            target = remote + relative_path
            parent = os.path.dirname(target)
            if not os.path.exists(parent):
                os.makedirs(parent)

            if os.path.exists(target) and \
               are_equals(filename, target):
                continue

            loginfo('Sync %s -> %s' % (filename, target))
            with file(filename, 'r') as f_in:
                with file(target, 'w') as f_out:
                    f_out.write(f_in.read())

    def delete_not_matched(self, remote):
        """Explore remote folder for files that match criteria"""
        for root, folders, files in os.walk(remote):
            for name in files:
                filename = os.path.join(root, name)
                if self._match(filename):
                    self.delete_one_file(remote, filename)

    def delete_one_file(self, remote, filename):
        """Check for local file deletion based on remote file path
        that match criteria."""
        relative_path = filename.split(remote)[-1]
        source = self.path + relative_path

        if self.delete_missing and \
           not os.path.exists(source):
            os.unlink(filename)
            return

        if not self.match_content(source):
            os.unlink(filename)


class PyTestHandler(EventHandler):
    """A simple Handler dummy class intended to launch py.tests
    when a python file is modified.
    """

    def match(self, event):
        "Determine if we can handle this event"
        return event.path.endswith('.py')


# register this module Handlers
register_handler(PelicanHandler)
register_handler(SyncHandler)
register_handler(PyTestHandler)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.basicConfig(level=logging.INFO)

    queue = DBQueue()
    flow = Flow(queue)

    flow.config()

    flow.run()
