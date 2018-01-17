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
from collections import OrderedDict, deque
from datetime import datetime, date
from getpass import getuser
import hashlib
import logging
from logging import info as loginfo, warning as logwarning
# from multiprocessing import Process
import os
# from pprint import pprint
import random
import re
import sys
import subprocess
import threading
import time
import types
from urllib import quote
import yaml
import traceback

from watcher import Watcher, event_key, Event

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
    if not os.path.exists(source):
        return False
    if not os.path.exists(target):
        return False

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
        self.idlecycles = 400
        self.configfile = ''
        # self.paths = list()


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

        self.watcher.add_watcher(handler.path, handler.inc_pat, handler.exc_pat)
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
            for handler in self.handlers:
                try:
                    handler.dispatch(event)
                except Exception, why:
                    print(handler)
                    traceback.print_exc()
                    logwarning(why)

    def idle(self):
        "Performs tasks that suit in idle loops from time to time"
        # now = time.time()
        # event = self.queue.get_last_event(now - 10)
        # if event:
        print "Do something idle ..."
        for handler in self.handlers:
            time.sleep(1)
            if handler._idle_thread:
                if handler._idle_thread.isAlive():
                    continue
                else:
                    handler._idle_thread.join()

            handler._idle_thread = threading.Thread(target=handler.on_idle)
            handler._idle_thread.start()
            # handler._idle_last_exec = 0

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
                    # create a instance with available params from __init__
                    func = handlers[klass].__init__.im_func.func_code
                    names = func.co_varnames[1:func.co_argcount]
                    args = dict([(k, state[k]) for k in names])
                    instance = handlers[klass](**args)

                    # and set the full state
                    instance.__setstate__(state)
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


MATCH_ALL = r'.*'
MATCH_NONE = r'(?!x)x'
SKIP_CONTENT = None
INCLUDE = 'include'
EXCLUDE = 'exclude'

class EventHandler(Persistent):
    "Base class for all EventHandlers"
    def __init__(self, path):
        """Base class for any event handler.

        Args:
            path (:obj:`string`, ): The `path` for monitoring into.
        """
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            raise OSError("%s path does not exits" % path)

        self.rules = dict()
        self.rules[INCLUDE] = dict()
        self.rules[EXCLUDE] = dict()
        # self._patterns = dict()

        self.flow = None

        self.path = os.path.abspath(path)
        self._idle_thread = None
        self._idle_last_exec = 0

        # for where in [INCLUDE, EXCLUDE]:
            # self._patterns[where] = re.compile(MATCH_NONE, re.I | re.DOTALL)

        # self.patterns_inc = r'|'.join(self.include)
        # self.patterns_exc = r'|'.join(self.exclude)
        # self.regexp_inc = re.compile(self.patterns_inc, re.DOTALL)
        # self.regexp_exc = re.compile(self.patterns_exc, re.DOTALL)

    def add_rule(self, where, name_pat, content_pat=SKIP_CONTENT):
        rules = self.rules[where]
        patterns = rules.setdefault(name_pat, list())
        if content_pat not in patterns:
            patterns.append(content_pat)
        # self._patterns[where] = re.compile(r'|'.join(rules), re.I | re.DOTALL)

    @property
    def inc_pat(self):
        return r'|'.join(self.rules[INCLUDE]) or MATCH_NONE

    @property
    def exc_pat(self):
        return r'|'.join(self.rules[EXCLUDE]) or MATCH_NONE

    def dispatch(self, event):
        if self.match(event):
            self.process(event)

    def match(self, event):
        "Determine if we can handle this event"
        return self._match(event.path)

    def _match(self, filename):
        if not filename.startswith(self.path):
            return False

        return self._match_content(filename)

        # return self._patterns[INCLUDE].search(filename) and \
               # not self._patterns[EXCLUDE].search(filename)

    def _match_content(self, filename):
        match = False
        for name_pat, content_patterns in self.rules[INCLUDE].items():
            if re.search(name_pat, filename):
                if self.__match_content(filename, content_patterns):
                    match = True
                    break

        for name_pat, content_patterns in self.rules[EXCLUDE].items():
            if re.search(name_pat, filename):
                if self.__match_content(filename, content_patterns):
                    match = False
                    break

        return match

    def __match_content(self, filename, content_patterns):
        """Analize file content that match any of the given rules."""
        if SKIP_CONTENT in content_patterns:
            return True

        if not os.path.exists(filename):
            return False

        regexp = re.compile(r'|'.join(content_patterns), re.DOTALL | re.I)

        with file(filename, 'rt') as f:
            for line in f.readlines():
                m = regexp.match(line)
                if m:
                    return m

        return False

    def process(self, event):
        """Search for a `on_xxxx` method that will handle the event
        in the subclass instance.
        """
        func_name = 'on_%s' % event.event
        func = getattr(self, func_name, None)
        if func:
            return func(event)

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
        aux = os.path.split(path)
        if aux[-1] == 'content':
            path = aux[0]

        EventHandler.__init__(self, path)

        # self.clear_output = True
        self.add_rule(INCLUDE, r'content/*\.py$')
        self.add_rule(INCLUDE, r'content/.*\.md$')

        self.add_rule(EXCLUDE, r'output/.*$')

        self._content_checked = False

    def on_created(self, event):
        "Fired when a new file is created"
        return self.on_modified(event)

    def on_modified(self, event, write=True):
        "Fired when a file is modified somehow"
        with file(event.path, 'rt') as f:
            lines = f.readlines()
            self.update_file(event, lines, write)

    def on_moved(self, event):
        "Fired when a file is renamed of moved"
        return self.on_modified(event)

    def on_deleted(self, event):
        "Fired when a file is deleted"
        self.make()

    def update_file(self, event, lines, write=True):
        """Update headers and variables of each file line"""
        contents = self.update_contents(event, lines)
        if not contents:
            template = self.find_template(event)
            if template:
                template.extend(lines)
                contents = self.update_contents(event, template)
                print ">> Adding template info for %s" % (event, )
                write = True

        if write:
            self.write_file(event, contents)
            self.make()

    def update_contents(self, event, lines):
        """Update headers and variables of each file line"""
        headers = dict()
        t = (event.date // 300) * 300
        headers['Modified'] = datetime.fromtimestamp(t) \
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

    def make(self):
        """Update the output contents for pelican.

        The event is passed as a reference.
        """
        self._idle_last_exec = time.time()

        args = ['pelican']
        proc = subprocess.Popen(args, cwd=self.path)

        proc.wait()
        return proc.returncode

    def on_idle(self):
        "Rebuild pelican blog from time to time"
        if time.time() - self._idle_last_exec > 240:
            self.make()

        # check content once in starting
        if not self._content_checked:
            for root, _, files in os.walk(self.path):
                for name in files:
                    path = os.path.join(root, name)
                    if path.endswith('.md') and self._match(path):
                        event = Event('modified', path, 0)
                        self.on_modified(event, write=False)
            self._content_checked = True




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

    def __init__(self, path, remotes):
        # TODO: better parsing arguments from command line
        EventHandler.__init__(self, path)

        delete_missing = False

        # self.rules = split_liststr(rules)
        self.remotes = split_liststr(remotes)
        # self.location = split_liststr(location)
        self.delete_missing = delete_missing

        self._run_once = False
        # self._rules.append(re.compile(regexp))

    def on_created(self, event):
        "Fired when a new file is created"
        return self.on_modified(event)

    def on_modified(self, event):
        "Fired when a file is modified somehow"
        if bool(self._match(event.path)) ^ \
           bool(self._match_content(event.path)):
            self.delete_one_file(event.path)
        else:
            self.sync_one_file(event.path)

    def on_deleted(self, event):
        "Fired when a file is deleted"
        # check self.delete_missing
        self.delete_one_file(event.path)

    def on_idle(self):
        "Performs syncing tasks"
        # p = Process(target=self.sync)
        # p.start()
        # p.join()
        if True or not self._run_once:  # TODO: review why some chanes aren't noticed
            print "Run Once on syncing"
            self.sync()  # for debugging
            self._run_once = True

    def sync(self):
        """Sync files that match criteria and remove any remote
        file that must not be there."""
        loginfo("Start Sync")
        # sync files to add
        for root, folders, files in os.walk(self.path):
            for name in files:
                filename = os.path.join(root, name)
                if self._match(filename):
                    event = Event('modified', filename, 0)
                    self.process(event)
                #else:
                # delete all files
                    # event = Event('deleted', filename, 0)


        # sync files that must be deleted
        if self.delete_missing:
            self._delete_missing()

    def sync_one_file(self, filename):
        """Try to sync a file into all remote folders."""
        # relative_path = filename.split(os.path.dirname(self.path))[-1]
        relative_path = filename.split(self.path)[-1]
        for remote in self.remotes:
            target = remote + relative_path
            parent = os.path.dirname(target)
            if not os.path.exists(parent):
                os.makedirs(parent)

            if are_equals(filename, target):
                continue

            loginfo('Sync %s -> %s' % (filename, target))
            with file(filename, 'rb') as f_in:
                with file(target, 'wb') as f_out:
                    f_out.write(f_in.read())

    def _delete_missing(self):
        """Delete remote files thar are not present in source folder"""
        for remote in self.remotes:
            # remote = os.path.join(remote, os.path.basename(self.path))
            for root, folders, files in os.walk(remote):
                for name in files:
                    filename = os.path.join(root, name)
                    relative_path =  filename.split(remote)[-1]
                    source = self.path + relative_path
                    if not os.path.exists(source):
                        os.unlink(filename)

    def delete_one_file(self, filename):
        """Check for local file deletion based on remote file path
        that match criteria."""
        # relative_path = filename.split(os.path.dirname(self.path))[-1]
        relative_path = filename.split(self.path)[-1]
        for remote in self.remotes:
            target = remote + relative_path
            if os.path.exists(target):
                os.unlink(target)
            dirname = os.path.dirname(target)
            if os.path.exists(dirname) and not os.listdir(dirname):
                os.rmdir(dirname)


class BlogSyncHandler(SyncHandler):
    """This class manage two or more trees to propagate changes
    across them.

    There have two methods:

    - Fast: uses the database queue to select only then know archives
            but may lack some files if flow was down when changes happened.

    - Deep: explore all file system tree

    The matching rules can be describe as a sequences of regular expressions
    that any file matching any of them, will be synchronized.
    """

    def __init__(self, path, remotes):
        # TODO: better parsing arguments from command line

        SyncHandler.__init__(self, path, remotes)

        self.add_rule(INCLUDE, r'.*\.svg$')
        self.add_rule(INCLUDE, r'.*\.py$')
        self.add_rule(INCLUDE, r'.*\.md$', r'Tags:\s*.*\W+(test)\W+.*')
        self.add_rule(INCLUDE, r'.*\.md$', r'Series:\s*.*\W+(vega)\W+.*')

        self.add_rule(EXCLUDE, r'.*\.css$')  # an example :)

        delete_missing = False


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
        if os.path.split(path)[-1] != 'content':
            path = os.path.join(path, 'content')

        EventHandler.__init__(self, path)

        self.env = Environment(
            loader=PackageLoader('flowmonitor', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        self.template = self.env.get_template('dashboard.tpl.md')
        self.last_generation = 0

        self.dashborad_file = os.path.join(self.path, 'dashboard.md')

        self.add_rule(INCLUDE, r'.*\.md$')


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
        r'(?P<priority>-|\+|\*)\s+\[(?P<done>.)\]\s+(?P<line>.*)$')

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
    keys = {' ': 'Waiting', None: 'Done', }
    return keys.get(status, '??')


class BackupHandler(EventHandler):
    """A simple Handler that create backups of .git repositories
    and upload to MEGA.
    """
    def __init__(self, path):
        # path = os.path.join(path, '.git')
        EventHandler.__init__(self, path)

        self.temp = '/tmp'
        self.remote_folder = '/backup'

        self.last_backup = dict()
        self.last_update = dict()
        self.last_working = dict()

        self._reset_remote_checking = 0

        self.add_rule(INCLUDE, r'\.git$')
        self.add_rule(EXCLUDE, r'nonexisting_path_1$')
        self.add_rule(EXCLUDE, r'nonexisting_path_2$')

    def on_idle(self):
        "Performs syncing tasks"

        # allow to inspect the remote site from time to time
        self._reset_remote_checking -= 1
        if self._reset_remote_checking < 0:
            self._reset_remote_checking = 50
            self.last_backup.clear()

        print("Idle on Backup: %s (reset in %s cycles)" %
              (self.path, self._reset_remote_checking))

        # check each repository for backing up
        for repo in folderiter(self.path, regexp=r'.*(\.git)$'):
            if self._match(repo):
                if self.must_update_git(repo):
                    print "BACKUP:\t%s" % repo
                    self.create_backup(repo)  # for debugging
                    self.rotate_files(repo)
            else:
                print "SKIP:\t%s" % repo
                foo = 1
        else:
            print("Backup: no commit from last time (reset in %s cycles)" %
                  self._reset_remote_checking)

    def must_update_git(self, path):
        """Check that there are new commits in git and
        the user seems to have stopped to modify working files
        before making a backup.
        """
        timestamp = self.get_git_timestamp(self.path)
        if timestamp > self.last_backup.get(path, 0):
            info = get_git_backup_info(path)

            bak = backup.MegaBackup()
            bak.start_daemon()
            remote_path = '%s/%s' % (self.remote_folder, info['remotename'])
            output, returncode = bak.execute('ls', remote_path)
            bak.stop_daemon()

            if returncode == 203:  # Couldn't find
                assert "Couldn't find" in output
            else:
                self.last_backup[path] = timestamp

        return timestamp > self.last_backup.get(path, 0)

    def get_git_timestamp(self, path):
        p = subprocess.Popen(
            ["git rev-list `git rev-parse HEAD` --max-count=1 --format='%ai'"],
            cwd=path,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        out, err = p.communicate()

        # '2017-07-12 08:46:33'
        timestamp = re.search(r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}',
                              out).group(0)
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        timestamp = time.mktime(timestamp.timetuple())

        return timestamp

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
            self.last_backup[path] = self.get_git_timestamp(path)
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
            # don't use the 'today' file, so means lock for future uploads
            if info['basename'] in all_files:
                all_files.remove(info['basename'])

            all_files.sort()
            matched_files = list()
            for name in all_files:
                m = regexp.match(name)
                if m:
                    m = m.groupdict()
                    date = date.fromordinal(int(m['ordinal']))
                    names = rotate_names(date, m['reponame'], m['ext'])
                    name = os.path.join(remote_path, name)
                    matched_files.append(name)
                    for target in names:
                        target = os.path.join(remote_path, target)
                        final[target] = name
            # delete the not used first
            # (in case of conexion interrpuption, is safer)
            used = set(final.values())
            for target in matched_files:
                if target not in used:
                    bak.execute('rm', target)

            # rename the 'survival' ones
            for placeholder, survival in final.items():
                bak.execute('cp', survival, placeholder)

            for survival in set(final.values()):
                bak.execute('rm', survival)

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

def get_base_name(basename, today):
    pass

def get_git_backup_info(path):
    "Returns some info about git backup based on repository path"
    if path.endswith('.git'):
        git_path = path
        parent = os.path.join(*os.path.split(path)[:-1])
        basename = os.path.basename(parent)
    else:
        git_path = os.path.join(path, '.git')
        basename = os.path.basename(path)

    basename = backup.MegaBackup.get_zip_name(basename)
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
register_handler(BlogSyncHandler)
register_handler(PyTestHandler)
register_handler(DashboardHandler)
register_handler(BackupHandler)

if __name__ == "__main__":
    # examples
    # pelican ~/agp/tpom pelican /tmp/remote/foo dashboard ~/agp/tpom backup ~/agp/tpom backup ~/Documents/me blogsync ~/agp/tpom /tmp/remote/foo
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.basicConfig(level=logging.INFO)

    # queue = DBQueue()
    # flow = Flow(queue)
    flow = Flow()
    flow.config()

    os.nice(20)  # unlx only
    flow.run()


