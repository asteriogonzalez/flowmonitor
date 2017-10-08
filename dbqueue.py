"""This module provide a Persiste Event Queue using sqlite
as DB support.
"""

from collections import namedtuple
import sqlite3
import time
import threading


Event = namedtuple('Event', ('date', 'path', 'event', 'folder',
                             'countdown', 'restart', 'path2'))

def event_key(event):
    "Provide a unique tuple that can be used as key for the event"
    return event.path, event.folder



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
