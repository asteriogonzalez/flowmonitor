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
import codecs
from collections import namedtuple, OrderedDict, deque
import datetime
from getpass import getuser
import hashlib
import logging
from logging import info as loginfo, warning as logwarning
# from multiprocessing import Process
import os
# from pprint import pprint
import random
import re
import sqlite3
import sys
import subprocess
import threading
import time
import types
from urllib import quote
import yaml

# from watchdog.observers import Observer
# from watchdog.events import FileSystemMovedEvent, \
     # FileSystemEventHandler  # , LoggingEventHandler

from watcher import Watcher, event_key

from jinja2 import Environment, PackageLoader, select_autoescape

import backup


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


Event_DB_OLD = namedtuple('Event', ('date', 'path', 'event', 'folder',
                             'countdown', 'restart', 'path2'))


# def event_key(event):
    # "Provide a unique tuple that can be used as key for the event"
    # return event.path, event.folder


class Task(object):
    def __init__(self, priority, done, text, fields, filename):
        self.priority = priority
        self.done = done
        self.text = text
        self.fields = fields
        self.filename = filename

    def __getattr__(self, key):
        return self.fields[key]


def exppath(path):
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    path = os.path.abspath(path)
    return path


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
            # print "Ingoring Duplicate event: %s" % (event, )
            pass
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

    def __init__(self):
        """This class needs a external queue to manage persistent events
        then creates:
        - an observer instantce that monitorize File System
        - an EventMonitor that handle earch event from Observer

        User handler are added later before running the insance.

        Args:
            queue (:obj:`instance`, ): `queue` that stores events.
        """
        self.queue = deque()
        self.watcher = Watcher()
        self.watcher.dispatch = self.dispatch
        self.ignored__ = dict()
        # self.monitor = EventMonitor(self)
        self.handlers = list()

        self.eventpolling = 0.2
        self.idlecycles = 200
        self.configfile = ''
        # self.paths = list()
        self._idle_running_klasses = dict()
        self._idle_queue = list()

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
        self.watcher.start()

        try:
            while True:
                self.idle()
                for counter in xrange(self.idlecycles):
                    time.sleep(self.eventpolling)  # non-locker style
                    self.next()

        except KeyboardInterrupt:
            loginfo('User Keyboard interrupt')

        self.close()

    def close(self):
        "Close all Flow activities and close the event queue"
        self.watcher.stop()
        self.watcher.join()
        # self.queue.close()

    def add(self, handler):
        """Add a handler that manage some events under a directory.
        """

        self.watcher.add_watcher(handler.path, handler.patterns)

        self.handlers.append(handler)
        handler.flow = self
        loginfo('Add handler: %s on %s' % (handler.__class__.__name__,
                                           handler.path))

    def next(self):
        "Try to execute the next event in queue"
        if not self.queue:
            pass
        else:
            event = self.queue.popleft()
            try:
                for handler in self.handlers:
                    if event.path.startswith(handler.path) and \
                       handler.match(event):
                        handler.process(event)
            except Exception, why:
                logwarning(why)

    def idle(self):
        "Performs tasks that suit in idle loops from time to time"
        # now = time.time()
        # event = self.queue.get_last_event(now - 10)
        # if event:
        print "Do something idle ..."
        for handler in self.handlers:
            if handler not in self._idle_queue:
                self._idle_queue.append(handler)

        for handler in list(self._idle_queue):
            klass = handler.__class__
            running = self._idle_running_klasses.get(klass)
            if running:
                if running._idle_thread:
                    if running._idle_thread.isAlive():
                        continue
                    else:
                        running._idle_thread.join()
                        running._idle_thread = None
                        self._idle_running_klasses.pop(klass)
                        running = None

            if not running:
                handler._idle_thread = threading.Thread(target=handler.on_idle)
                handler._idle_thread.start()
                self._idle_running_klasses[klass] = handler
                self._idle_queue.remove(handler)
                time.sleep(1)

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


    def dispatch(self, event):
        key = event_key(event)
        if self.ignored__.pop(key, None):
            print "Ignoring autogenerated event: %s" % (event, )
        else:
            print "FLOW: Procesing: %s: %s at %s from FLOW" % event
            self.queue.append(event)

    def ignore(self, event):
        key = event_key(event)
        self.ignored__[key] = event

# class EventMonitor(FileSystemEventHandler):
    # """Logs all the events captured."""

    # def __init__(self, flow):
        # self.flow = flow

    # def on_any_event(self, event):
        # now = time.time()
        # if isinstance(event, FileSystemMovedEvent):
            # ev, path2, path, folder = event.key
        # else:
            # ev, path, folder = event.key
            # path2 = ''

        # # ev = self.tran[ev]
        # path = os.path.abspath(path)
        # ev = Event(now, path, ev, folder, 5, 10, path2)
        # flow.queue.push(ev)
        # # for handler in self.flow.handlers:
            # # if ev.path.startswith(handler.path):
                # # if handler.match(ev):
                    # # flow.queue.push(ev)


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
        self.patterns = r'|'.join([r'.*\%s$' % ext for ext in self.extensions])
        self.regexp = re.compile(self.patterns, re.DOTALL)

        self._idle_thread = None


    def match(self, event):
        "Determine if we can handle this event"
        return not self.extensions or self.regexp.match(event.path)

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
        print "Idle on %s" % self.__class__.__name__


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
        # TODO: better parsing arguments from command line
        extensions = ['.md', '.py']
        if os.path.split(path)[-1] != 'content':
            path = os.path.join(path, 'content')

        EventHandler.__init__(self, path, extensions)

        self.project_root = os.path.split(self.path)[0]
        # self.clear_output = True

    def on_created(self, event):
        "Fired when a new file is created"
        return self.on_modified(event)

    def on_modified(self, event):
        "Fired when a file is modified somehow"
        with file(event.path, 'rt') as f:
            lines = f.readlines()
            self.update_file(event, lines)

    def on_moved(self, event):
        "Fired when a file is renamed of moved"
        return self.on_modified(event)

    def on_deleted(self, event):
        "Fired when a file is deleted"
        self.make(event)

    def update_file(self, event, lines):
        """Update headers and variables of each file line"""
        contents = self.update_contents(event, lines)
        if not contents:
            template = self.find_template(event)
            if template:
                template.extend(lines)
                contents = self.update_contents(event, template)

        self.write_file(event, contents)
        self.make(event)

    def update_contents(self, event, lines):
        """Update headers and variables of each file line"""
        headers = dict()
        t = (event.date // 300) * 300
        headers['Modified'] = datetime.datetime.fromtimestamp(t) \
            .strftime('%Y-%m-%d %H:%M')
        n = self.update_headers(lines, headers)

        if n <= 0:
            # Seems to be a new article, without any headers
            # so insert the template and append existing contents
            # to the end.
            return []

        if not self._re_templates.match(event.path):
            replace = dict()
            replace[r'\$random\$'] = (random.randint, 0, 1000)
            replace[r'\$date\$'] = headers['Modified']
            replace[r'\$user\$'] = getuser()
            replace[r'\$author\$'] = getuser()
            self.update_replacements(lines, replace)

        return lines

    def update_headers(self, lines, replace):
        "Replace some file headers and write down the new content"
        n = 0
        for i, line in enumerate(lines):
            m = self._re_headers.match(line)
            if m:
                value = replace.get(m.groupdict().get('name'))
                if value is not None:
                    line = self._re_headers.sub(
                        r'\g<name>: %s\n' % value, line)
                    lines[i] = line
                    n += 1
        return n

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
        self.flow.ignore(event)

        with file(event.path, 'wt') as f:
            f.writelines(lines)

    def find_template(self, event):
        """Find a template"""
        newname = event.path.split(self.path)[-1].lower()
        ext = os.path.splitext(event.path)[-1]
        new_tokens = set(split_string(newname))
        best_match = 0
        lines = ['this is an empty article!']

        for root, folders, files in os.walk(self.project_root):
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

        # Pelican delete all content, preserving the folder 'output'
        # so this code is not necessary
        # if False and self.clear_output:
            # output = os.path.join(self.project_root, 'output')
            # for filename in fileiter(output, r'.*\.(css|html|js|jpg|png|gif|svg|ico|json)$'):
                # print "Removing:", filename
                # os.unlink(filename)

        args = ['pelican']
        proc = subprocess.Popen(args, cwd=self.project_root)

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

    def __init__(self, path):
        # TODO: better parsing arguments from command line
        extensions = '.md'
        remotes = '/tmp/remote'
        rules = r'Tags:\s*.*\W+(foo)\W+.*'
        location = 'ai'
        delete_missing = False

        # EventHandler.__init__(self, path, extensions)
        EventHandler.__init__(self, path)
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
        return EventHandler(self, event) and event.path.endswith('.py')


class DashboardHandler(EventHandler):
    """A simple Handler that regenerate a Task Dashboard.
    """
    def __init__(self, path):
        path = os.path.join(path, 'content')
        extensions = '.md'
        EventHandler.__init__(self, path, extensions)

        self.env = Environment(
            loader=PackageLoader('flowmonitor', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        self.template = self.env.get_template('dashboard.tpl.md')
        self.last_generation = 0

        self.dashborad_file = os.path.join(self.path, 'dashboard.md')

    def on_idle(self):
        "Performs syncing tasks"

        print "Idle on Dashboard"
        if self.must_update_git():
            # p = Process(target=self.sync)
            # p.start()
            # p.join()
            self.create_dashboard()  # for debugging
        else:
            print "Nothing has change from last time ..."

    def must_update_git(self):
        for filename in fileiter(self.path, '.*\.md$'):
            if filename == self.dashborad_file:
                continue

            mtime = os.stat(filename).st_mtime
            if mtime > self.last_generation:
                self.last_generation = time.time()
                return True

    def create_dashboard(self):
        # Build all META info from MD files
        meta = dict()
        for filename in fileiter(self.path, '.*md'):
            meta[filename] = metainfo(filename)

        all_tags = get_tags(meta)

        # Create a sorted LIST of ALL tasks
        # sort_field = 'modified'
        sort_field = 'date'
        all_tasks = sort_task(meta, sort_field)

        # prepare context for rendering
        context = locals()
        context['len'] = list.__len__
        context['translate_done'] = translate_done

        # render
        output = self.template.render(context)
        # output = output.replace('\n\n', '\n')

        # save to dashboard Markdown file
        dashboard = os.path.join(self.path, 'dashboard.md')
        dashboard = exppath(dashboard)
        with codecs.open(dashboard, 'w', 'utf-8') as f:
            f.write(output)


def fileiter(path, regexp=None):
    if isinstance(regexp, types.StringTypes):
        regexp = re.compile(regexp, re.DOTALL | re.VERBOSE)

    for root, _, files in os.walk(exppath(path)):
        for name in files:
            filename = os.path.join(root, name)
            if not regexp:
                yield filename
            elif regexp.match(filename):
                yield filename

def folderiter(path, regexp=None):
    if isinstance(regexp, types.StringTypes):
        regexp = re.compile(regexp, re.DOTALL | re.VERBOSE)

    for root, folders, _ in os.walk(exppath(path)):
        for name in folders:
            filename = os.path.join(root, name)
            if not regexp:
                yield filename
            elif regexp.match(filename):
                yield filename



def get_modified(path, since=0, regexp=None):
    if os.path.isdir(path):
        for name in fileiter(path, regexp):
            mdate = os.stat(name).st_mtime
            if mdate > since:
                yield name, mdate
    else:
        yield path, os.stat(path).st_mtime


def get_url(field):
    url = field.get('slug')
    if url:
        # url = url.replace(' ', '%20')
        url = quote(url)
    else:
        # url = field.get('title', '')
        url = field.get('title', '')
        url = url.lower().replace(' ', '-')
    lang = field.get('lang')
    if lang and lang != 'en':
        url += '-' + lang
    url += '.html'
    return url


def collect_info(filename):
    field = dict()
    tasks = list()

    re_task = re.compile(
        r'(?P<priority>-|\*)\s+\[(?P<done>.)\]\s+(?P<line>.*)$')

    f = codecs.open(filename, 'r', 'utf-8')
    for line in f.readlines():
        # fields
        m = PelicanHandler._re_headers.match(line)
        if m:
            k, v = m.groups()
            field[k.lower()] = v
        # tasks
        m = re_task.match(line)
        if m:
            tasks.append(m.groups())
            # done, text = m.groups()
            # tasks.append((done.strip().lower(), text))

    field['url'] = get_url(field)
    return field, tasks


def metainfo(filename):
    fields, tasks = collect_info(filename)
    tags = [t.strip() for t in fields.get('tags', '').lower().split(',')]
    tags = [t for t in tags if t]  # remove blank tags
    tags.sort()
    fields['tags'] = tags
    return fields, tasks


def get_tags(meta):
    all_tags = set()
    for filename, info in meta.items():
        fields, tasks = info
        all_tags.update(fields['tags'])
    all_tags = list(all_tags)
    all_tags.sort()
    return all_tags


def task_iterator(meta):
    for filename, info in meta.items():
        fields, tasks = info
        for priority, done, text in tasks:
            yield Task(priority, done, text, fields, filename)


class TaskGroups(OrderedDict):
    pass


class GroupableTasks(list):
    def group_by(self, field, categories, once=False, exact=False,
                 notmaching=False, skip_empty=True):
        groups = TaskGroups()
        used = set()
        for cat in categories:
            grp = GroupableTasks()
            for i, task in enumerate(self):
                if once and i in used:
                    continue
                match = False
                value = getattr(task, field)
                if isinstance(value, types.ListType):
                    if cat in value:
                        match = True
                elif exact:
                    if value == cat:
                        match = True
                else:
                    if value <= cat:
                        match = True

                if match:
                    grp.append(task)
                    used.add(i)

            if len(grp) > 0 or not skip_empty:
                groups[cat] = grp

        if notmaching:
            notused = GroupableTasks()
            for i, task in enumerate(self):
                if i not in used:
                    notused.append(self[i])
            groups[None] = notused

        return groups


def sort_task(meta, field):
    "Sort tasks by field"
    field = field.lower()
    tasks = [t for t in task_iterator(meta)]
    tasks = GroupableTasks(tasks)

    def sort(a, b):
        "Sort tasks by field"
        va = a.fields.get(field)
        vb = b.fields.get(field)
        if va is None:
            return -1
        if vb is None:
            return 1

        return cmp(va, vb)
    tasks.sort(sort)
    return tasks


def group_task(tasks, field, ranges):
    """Group tasks by field and ranges, and task can appears
    in every matching groups"""

    # split into groups
    groups = dict()
    tasks = list(tasks)
    for r in ranges:
        grp = list()
        for task in tasks:
            value = task.fields[field]
            if isinstance(value, types.ListType):
                if r in value:
                    grp.append(task)
            elif value <= r:
                grp.append(task)

        groups[r] = grp

    return groups


def print_groups(groups):
    "Helper function for printing groups in console"
    for tag, grp in groups.items():
        print "-", tag, "-" * 70
        for task in grp:
            print '- [%s]: %s %s' % (task.done, task.text, task.tags)


def translate_done(status):
    "Translate task 'done' attribute to a human readable way"
    keys = {' ': 'Next', None: 'Done', }
    return keys.get(status, '??')


class BackupHandler(EventHandler):
    """A simple Handler that create backups of .git repositories
    and upload to MEGA.
    """
    def __init__(self, path):
        # path = os.path.join(path, '.git')
        extensions = '.md'
        EventHandler.__init__(self, path, extensions)

        self.temp = '/tmp'
        self.remote_folder = '/backup'

        self.last_backup = dict()
        self.last_update = dict()
        self.last_working = dict()

    def on_idle(self):
        "Performs syncing tasks"

        print("Idle on Backup: %s" % self.path)
        for repo in folderiter(self.path, regexp=r'.*(\.git)$'):
            if self.must_update_git(repo):
                self.create_backup(repo)  # for debugging
                self.rotate_files(repo)

        else:
            print("Nothing has change from last time ...")

    def must_update_git(self, path):
        """Check that there are new commits in git and
        the user seems to have stopped to modify working files
        before making a backup.
        """
        info = get_git_backup_info(path)

        bak = backup.MegaBackup()
        bak.start_daemon()
        remote_path = '%s/%s' % (self.remote_folder, info['remotename'])
        output, returncode = bak.execute('ls', remote_path)
        bak.stop_daemon()

        if returncode == 203:  # Couldn't find
            assert "Couldn't find" in output
            return True

    def get_git_timestamp(self, path):
        p = subprocess.Popen(
            # ["git", "rev-list" "--format=format:'%ai'",
             # "--max-count=1", "`git rev-parse HEAD`"],
            ["git rev-list `git rev-parse HEAD` --max-count=1 --format='%ai'"],
            cwd=path,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        out, err = p.communicate()

        # '2017-07-12 08:46:33'
        timestamp = re.search(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}', out).group(0)
        timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.mktime(timestamp.timetuple())

        self.last_update[path] = timestamp

        if self.last_backup.setdefault(path, 0) > self.last_update[path]:
            return False

        return True

    def must_update(self, path):
        """Check that there are updates in working files
        before making a backup.
        """
        self.last_update[path] = max(
            get_modified(path),
            key=key1)[1]

        if self.last_backup.setdefault(path, 0) > self.last_update[path]:
            return False

        parent = os.path.join(*os.path.split(path)[:-1])
        self.last_working[path] = max(
            get_modified(parent, since=0),
            key=key1)[1]

        # make a backup 30 min later than user has modified any file
        delay = 1800
        return time.time() - self.last_working[path] > delay


    def create_backup(self, path):
        "Create a New backup for the supervided path"
        print("Creating a new Backup for %s" % path)

        bak = backup.MegaBackup()
        zipfile = bak.compress_git(path)
        if zipfile:
            info = get_git_backup_info(path)
            remote_path = os.path.join(self.remote_folder, info['remotename'])

            bak.start_daemon()
            bak.upload(zipfile, remote_path, remove_after=True)
            bak.stop_daemon()
            self.last_backup[path] = time.time()
        else:
            print("Error making backup file")

    def rotate_files(self, path):
        regexp = re.compile(r'(?P<reponame>\w+)-(?P<ordinal>\d+)(?P<ext>.*)$',
                            re.DOTALL)
        info = get_git_backup_info(path)

        bak = backup.MegaBackup()
        bak.start_daemon()

        remote_path = '%s/%s' % (self.remote_folder, info['reponame'])
        output, returncode = bak.execute('ls', remote_path)
        if returncode == 0:
            # get already remote files info
            # and calculate the final state of all pending ones
            final = dict()
            all_files = output.splitlines()
            all_files.sort()
            for name in all_files:
                m = regexp.match(name)
                if m:
                    m = m.groupdict()
                    date = datetime.date.fromordinal(int(m['ordinal']))
                    names = rotate_names(date, m['reponame'], m['ext'])
                    name = os.path.join(remote_path, name)
                    for target in names:
                        target = os.path.join(remote_path, target)
                        final[target] = name
            # delete the not used first
            # (in case of conexion interrpuption, is safer)
            used = set(final.values())
            for target in all_files:
                target = os.path.join(remote_path, target)
                if target not in used:
                    bak.execute('rm', target)

            # rename the 'survival' ones
            for placeholder, survival in final.items():
                bak.execute('mv', survival, placeholder)


        bak.stop_daemon()


def key1(x): return 1


def rotate_names(date, name, ext):
    "Create a set of rotate names from a given path"
    weekday = date.weekday()
    week = date.day // 7
    month = date.month
    year = str(date.year)[2:]

    return (''.join([name, '.d%s' % weekday, ext]),
            ''.join([name, '.w%s' % week, ext]),
            ''.join([name, '.m%s' % month, ext]),
            ''.join([name, '.y%s' % year, ext]))


def get_git_backup_info(path):
    "Returns many info about git back based on repository path"
    if path.endswith('.git'):
        git_path = path
        parent = os.path.join(*os.path.split(path)[:-1])
        basename = os.path.basename(parent)
    else:
        git_path = os.path.join(path, '.git')
        basename = os.path.basename(path)

    today = datetime.date.today()
    basename = '%s-%s.git.7z' % (basename, today.toordinal())
    reponame = os.path.basename(parent)
    remotename = os.path.join(reponame, basename)

    info = dict(
        git_path=git_path,
        parent=parent,
        basename=basename,
        reponame=reponame,
        remotename=remotename
    )
    return info

# register this module Handlers
register_handler(PelicanHandler)
register_handler(SyncHandler)
register_handler(PyTestHandler)
register_handler(DashboardHandler)

register_handler(BackupHandler)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.basicConfig(level=logging.INFO)

    # queue = DBQueue()
    # flow = Flow(queue)
    flow = Flow()
    flow.config()

    flow.run()
