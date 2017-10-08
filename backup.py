# from __future__ import print_function
"""
This module provides Backup capabilities into your MEGA account
of any folder, but is focused on tracking changes git repositories.

Dependences
============
The MegaBackup class uses the mega-cmd packege that you can install
from the application section in http://mega.nz

7z command line program is used to create encrypted compressed files.

Rotation Policy
================
Rotation policy is also supported:

- 7-daily (mon-sun) :       e.g. flowmonitor.git.d6.7z
- 1-per-week of the month:  e.g. flowmonitor.git.w0.7z
- 1 per month:              e.g. flowmonitor.git.m9.7z
- 1 per year:               e.g. flowmonitor.git.y17.7z

So we need 7 + 5 + 12 = 24 files to cover a whole year.

In each backup the dairy backup is generated and then is cloned
modifying the rotation names, so the compressed file only is sent once
and hence all files correspond with the last update.

As soon a new day comes, some of the previous backups files will be left
behind creating the rotation sequence.

Login into MEGA
=================

Currently user must manually login into MEGA account for 1st time
using

``` bash
$ mega-cmd
MEGA CMD> login your@email.com
Password: xxxxx
[info: 10:13:01] Fetching nodes ...
<###############################|(1/1 MB: 100.00 %)
[info: 10:13:05] Login complete as your@email.com
your@email.com:/$ quit
[info: 10:13:37] closing application ...
$
```


"""
import datetime
import subprocess
import os
import re
import time
import threading

from flowmonitor import get_modified
# TODO: return list of files not copied


class Runner(object):
    "A timeout runner that uses threading to control timeouts"
    def __init__(self, cmd, timeout=120, **keywords):
        self.cmd = cmd
        self.timeout = timeout
        self.keywords = keywords
        self.proc = None
        self.stdout = None
        self.stderr = None

    def go(self):
        "Execute the command in a separate thread"
        thread = threading.Thread(target=self.target)
        thread.start()
        thread.join(self.timeout)
        if thread.is_alive():
            print('*** TIMEOUT %s ****' % self.timeout)
            if self.proc:  # subprocess has created
                self.proc.terminate()
            thread.join()
            return None, None

        return self.stdout, self.proc.returncode

    def target(self):
        "This is the main function for the thread"
        # self.process = subprocess.Popen(self.cmd, shell=True)
        self.proc = subprocess.Popen(
            self.cmd, shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, **self.keywords)

        self.stdout, self.stderr = self.proc.communicate()


def run(cmd, **kw):
    "Run a command and returns stdout and return code"
    _ = kw.pop('timeout', None)  # not valid in 2.7
    proc = subprocess.Popen(
        cmd, shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, **kw)

    stdout, _ = proc.communicate()
    return stdout, proc.returncode


class MegaBackup(object):
    """This class encapusulates the backup capabilities and
    MEGA commands to upload and manage the remote files.
    """
    regsize = re.compile(r'\S+\s+(?P<size>\d+)')
    compress_cmd = ['nice', '7z', 'a', '-phello', '-mhe', '-mx9', '-mmt']

    def __init__(self, credentials=None):  # TODO: user credentials
        self.credentials = credentials
        self.daemon = None

    def start_daemon(self):
        "Launch the mega-cmd daemon and login into the system"
        args = ['mega-cmd']
        self.daemon = subprocess.Popen(args, shell=False, cwd='/tmp')
        if not self.daemon.pid:
            raise RuntimeError("Unable to launch daemon login")
        time.sleep(1)

    def stop_daemon(self):
        "Stop the mega-cmd daemon"
        self.daemon.terminate()

    def execute(self, *args, **kw):
        "Execute a shell alike functions: cp, ls, put, etc"
        cmd = ['mega-exec']
        cmd.extend(args)
        kw.setdefault('timeout', 600)

        print "-" * 70
        print "EXEC: %s" % ' '.join(cmd)
        try:
            # output = subprocess.check_output(cmd, **kw)
            # output, returncode = Runner(cmd, **kw).go()
            output, returncode = run(cmd, **kw)
            time.sleep(0.02)  # sometimes threads are locked. A pause will help
        except Exception, why:
            print(why)
            print('Waiting 5 secs before continue ...')
            time.sleep(5)
            raise why

        print "OUT: %s" % output
        print "RET: [%s]" % returncode

        time.sleep(1)

        return output, returncode

    def upload_folder(self, folder, destination, remove_after=False):
        """Upload a entire folder into MEGA.
        """
        dirname = os.path.dirname(folder)
        for root, _, files in os.walk(folder):
            for name in files:
                path = os.path.join(root, name)
                where = os.path.join(destination, path.split(dirname)[-1][1:])

                self.replace(path, where)

                if remove_after:
                    os.unlink(path)

    def upload(self, localfile, destination, remove_after=False):
        """Upload a file into MEGA and create rotate file policy is needed"""
        ext = os.path.splitext(destination)
        if not ext[-1]:
            destination = os.path.join(
                destination,
                os.path.basename(localfile))

        result = self.replace(localfile, destination)

        if remove_after:
            os.unlink(localfile)

        return result

    def safe_cmd(self, cmd, source, target, delete_source=True):
        "move only if target don't exists. delete source is optional"
        output, returncode = self.execute('ls', target)
        if returncode != 0:
            self.execute(cmd, source, target)
        elif delete_source:
            self.execute('rm', source)

    def replace(self, path, where, quick=True):
        "Replace a file safely in remote server"
        print(">> REPLACE: %s -> %s" % (path, where))
        if os.path.exists(path):
            remove = False
            if quick:
                try:
                    output, returncode = self.execute('du', where)
                    if not returncode:
                        match = self.regsize.match(output)
                        if match:
                            remote_size = int(match.group(1))
                            local_size = os.stat(path).st_size
                            if remote_size == local_size:
                                return
                            remove = True

                except subprocess.CalledProcessError:
                    pass
            if not quick or remove:
                try:
                    print("** REMOVING: %s **" % where)
                    self.execute('rm', where)
                except subprocess.CalledProcessError:
                    pass

            try:
                result = self.execute('put', '-c',
                                      path,
                                      where,
                                      # os.path.dirname(where)
                                      )
                time.sleep(1)  # nice with mega
            except subprocess.CalledProcessError:
                pass

        return result

    def compress(self, path, zipfile):
        """Compress a folder and check if someone has modified
        the content meanwhile."""
        for attempt in range(5):
            def key(_):
                return 1
            last_0 = max(get_modified(path, since=0), key=key)[1]

            cmd = clone_list(self.compress_cmd, zipfile, path)
            stdout, returncode = run(cmd)

            last_1 = max(get_modified(path, since=0), key=key)[1]
            if last_0 == last_1:
                return True
            print('contents are update while compressing folder, retry %s'
                  % attempt)
            time.sleep(1)

        return False

    def compress_git(self, path):
        "Compress the git repository of folder"
        if path.endswith('.git'):
            git_path = path
            parent = os.path.join(*os.path.split(path)[:-1])
            basename = os.path.basename(parent)
        else:
            git_path = os.path.join(path, '.git')
            basename = os.path.basename(path)

        if os.path.exists(git_path):
            # use garbage collector for this repository
            # before making backup
            run(['git', 'gc'], cwd=path)

            today = datetime.date.today()
            basename = '%s-%s.git.7z' % (basename, today.toordinal())
            zipfile = os.path.join('/tmp/', basename)
            if self.compress(git_path, zipfile):
                cmd = clone_list(self.compress_cmd, zipfile)
                extrafiles = ['.gitignore', '.gitmodules']
                for name in extrafiles:
                    name = os.path.join(parent, name)
                    if os.path.exists(name):
                        cmd.append(name)
                run(cmd)

                return zipfile


def rotatenames(path):
    "Create a set of rotate names from a given path"
    name, ext = os.path.splitext(path)
    now = datetime.date.today()
    weekday = now.weekday()
    week = now.day // 7
    month = now.month
    year = str(now.year)[2:]

    return (''.join([name, '.d%s' % weekday, ext]),
            ''.join([name, '.w%s' % week, ext]),
            ''.join([name, '.m%s' % month, ext]),
            ''.join([name, '.y%s' % year, ext]))


def clone_list(alist, *args):
    result = list(alist)
    result.extend(args)
    return result


if __name__ == '__main__':

    # work = os.path.dirname(__file__)
    # backup = MegaBackup()

    # zipfile = backup.compress_git(work)

    # backup.start_daemon()

    # backup.upload(zipfile, '/test/', rotate=True, remove_after=True)

    # ls = backup.execute('ls')
    # print(ls)
    # filename = os.path.abspath(__file__)
    # r = backup.execute('put', filename, '/test/')

    # backup.upload_folder(work, '/test/')

    # backup.stop_daemon()

    print('-End-')
