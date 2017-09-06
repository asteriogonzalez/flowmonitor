import os
import time
import datetime
import re
from threading import Thread
from collections import OrderedDict, namedtuple


def print_mdate(path):
    t = os.stat(path).st_mtime
    s = datetime.datetime.fromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S')
    print "%-80s %s" % (path, s)


def exppath(path):
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    path = os.path.abspath(path)
    return path + os.path.sep


Event = namedtuple('Event', ('event', 'path', 'date'))


def event_key(event):
    "Provide a unique tuple that can be used as key for the event"
    return event[:2]

class Watcher(Thread):
    def __init__(self, *args, **kw):
        self.watchers = dict()
        self.unique_paths = []
        self.favorite_files = OrderedDict()
        self.max_favorites = 100
        self.last_modified = time.time()
        self.running = True
        self.idle = 0.0001
        self.hard_idle = 0.001
        self.soft_idle = 0.1
        Thread.__init__(self, *args, **kw)

    def add_watcher(self, path, pattern='.*'):
        path = exppath(path)
        patterns, fullpatt, compiled = self.watchers.setdefault(
            path, ['', '', None])

        if patterns:
            patterns += '|' + pattern
        else:
            patterns = pattern

        # regexp = r'%s(%s)' % (path, patterns)
        regexp = patterns
        self.watchers[path] = [patterns, regexp, re.compile(regexp, re.DOTALL)]

        # add unique paths for searching
        new = False
        for p in list(self.unique_paths):
            if path in p:  # subtree
                self.unique_paths.remove(p)
                new = True
        else:
            new = True

        if new:
            self.unique_paths.append(path)

    def run(self):
        print ">> starting of monitorize ..."
        try:
            while self.running:
                time.sleep(0.3)
                print "...."
                for event in self.next():
                    self.dispatch(event)
        except KeyboardInterrupt:
            self.running = False

        print "<< exiting form monitorize ..."

    def stop(self):
        self.running = False

    def next(self):
        # balance check iterators monitoring
        try:
            queue = [self.fileiterator(), self.favoriteiterator()]
            while self.running:
                gen = queue.pop(0)
                try:
                    event = gen.next()
                    queue.append(gen)
                    if event.path:
                        if self.add_favorite(event):
                            yield event
                            self.last_modified = min(self.last_modified, event.date)
                except StopIteration:
                    func = getattr(self, gen.gi_code.co_name)
                    queue.append(func())  # restart again

                time.sleep(self.idle)

        except KeyboardInterrupt:
            self.running = False

    def fileiterator(self):
        """Iterate for """
        since = self.last_modified  # make a copy at the begining
        for top in self.unique_paths:
            for root, _, files in os.walk(top):
                for name in files:
                    path = os.path.join(root, name)
                    if path in self.favorite_files:
                        # let the other generator notifies the changes
                        continue

                    mtime = os.stat(path).st_mtime
                    if mtime > since and self.match_watcher(path):
                        yield Event('modified', path, mtime)
                        self.last_modified = max(self.last_modified, mtime)
                else:
                    # after process a folder, we return control to
                    # collaborate in multitasking
                    # print "ROOT:", root
                    yield Event('none', '', 0)

        self.relax_search()

    def favoriteiterator(self):
        since = self.last_modified  # make a copy at the begining
        for path, last_time in self.favorite_files.items():
            if os.path.exists(path):
                mtime = os.stat(path).st_mtime
                if mtime > last_time and mtime > since:
                    yield Event('modified', path, mtime)
                    self.last_modified = max(self.last_modified, mtime)
            else:
                self.favorite_files.pop(path)
                yield Event('deleted', path, time.time())

    def add_favorite(self, event):
        """Add files for high frequency monitoring that match
        some handler and determine if any handler would attend
        the event.
        """
        if event.event in ('deleted'):
            self.favorite_files.pop(event.path, None)
            return True

        if self.favorite_files.pop(event.path, None):
            self.favorite_files[event.path] = event.date
            return True

        if self.match_watcher(event.path):
            self.favorite_files[event.path] = event.date
            self.relax_search()

            if len(self.favorite_files) > self.max_favorites:
                # # remove some the older ones
                # times = self.favorite_files.values()
                # times.sort()
                # older = times[0]  # use 1, 2 if you want to delete more
                # for key, value in self.favorite_files.items():
                    # if value <= older:
                        # self.favorite_files.pop(key)

                killed = self.favorite_files.keys()[0]
                self.favorite_files.pop(killed)

            return True
        return False

    def match_watcher(self, path):
        for top, (pattern, _, regexp) in self.watchers.items():
            if top not in path:
                continue
            if regexp.match(path):
                return top

    def relax_search(self):
        idle = self.idle * 1.5
        if idle > self.soft_idle:
            idle = self.hard_idle

        print "idle: %s" % idle
        self.idle = idle

    def dispatch(self, event):
        print "Procesing: %s at %s" % event


if __name__ == '__main__':
    watcher = Watcher()
    watcher.add_watcher('~/Documents/me', pattern='.*\.md$')
    watcher.add_watcher('~/Documents/me', pattern='.*\.py$')

    watcher.add_watcher('~/Documents/tpom/content', pattern='.*\.md$')
    watcher.add_watcher('~/Documents/tpom', pattern='.*\.py$')

    watcher.start()

    time.sleep(3600)
    watcher.running = False

    print "-End-"