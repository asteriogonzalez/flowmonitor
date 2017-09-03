#from __future__ import print_function
import datetime
import subprocess
import os
import re
import time
import threading

from flowmonitor import get_modified
# TODO: return list of files not copied

ERR_TIMEOUT = -1000

class Runner(object):
    def __init__(self, cmd, timeout=120, **kw):
        self.cmd = cmd
        self.timeout = timeout
        self.kw = kw
        self.proc = None
        self.stdout = None
        self.stderr = None

    def go(self):
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
        # self.process = subprocess.Popen(self.cmd, shell=True)
        self.proc = subprocess.Popen(
            self.cmd, shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, **self.kw)

        # print("> subprocess: %s created" % self.cmd)

        self.stdout, self.stderr = self.proc.communicate()
        # print("< subprocess: %s finished [%s]" % (self.cmd, self.proc.returncode))

def run(cmd, **kw):
    _ = kw.pop('timeout', None)  # not valid in 2.7
    proc = subprocess.Popen(
        cmd, shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, **kw)

    stdout, stderr = proc.communicate()
    return stdout, proc.returncode


class MegaBackup(object):
    regsize = re.compile(r'\S+\s+(?P<size>\d+)')

    def __init__(self, credentials=None):  # TODO: user credentials
        self.credentials = credentials
        self.daemon = None

    def start_daemon(self):
        "Launch the mega-cmd daemon and login into the system"
        args = ['mega-cmd']
        self.daemon = subprocess.Popen(args, shell=False, cwd='/tmp')
        if not self.daemon.pid:
            raise RuntimeError("Unable to launch daemon login")

    def stop_daemon(self):
        self.daemon.terminate()

    def execute(self, *args, **kw):
        cmd = ['mega-exec']
        cmd.extend(args)
        kw.setdefault('timeout', 600)
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

        return output, returncode

    def upload_folder(self, folder, destination, remove_after=False):

        dirname = os.path.dirname(folder)
        for root, _, files in os.walk(folder):
            for name in files:
                path = os.path.join(root, name)
                where = os.path.join(destination, path.split(dirname)[-1][1:])

                self.replace(path, where)

                if remove_after:
                    os.unlink(path)

    def upload(self, localfile, destination, rotate=False, remove_after=False):
        ext = os.path.splitext(destination)
        if not ext[-1]:
            destination = os.path.join(destination, os.path.basename(localfile))

        if rotate:
            names = rotatenames(destination)
            result = self.replace(localfile, names[0])
            for target in names[1:]:
                self.execute('cp', names[0], target)
        else:
            result = self.replace(localfile, destination)

        if remove_after:
            os.unlink(localfile)

        return result

    def replace(self, path, where, quick=True):
        "Replace a file safely in remote server"
        print(">> %s" % where)
        if os.path.exists(path):
            remove = False
            if quick:
                try:
                    output, returncode = self.execute('du', where)
                    if not returncode:
                        m = self.regsize.match(output)
                        if m:
                            remote_size = int(m.group(1))
                            local_size = os.stat(path).st_size
                            if remote_size == local_size:
                                return
                            remove = True

                except subprocess.CalledProcessError, why:
                    pass
            if not quick or remove:
                try:
                    print("** REMOVING: %s **" % where)
                    self.execute('rm', where)
                except subprocess.CalledProcessError, why:
                    pass

            try:
                result = self.execute('put', '-c', path, where)
                time.sleep(1)  # nice with mega
            except subprocess.CalledProcessError, why:
                pass

        return result

    def compress(self, path, zipfile):
        for attempt in range(5):
            def key(x): return 1
            last_0 = max(get_modified(path, since=0), key=key)[1]

            cmd = ['7z', 'a', '-phello', '-mhe', zipfile, path]
            stdout, returncode = run(cmd)

            last_1 = max(get_modified(path, since=0), key=key)[1]
            if last_0 == last_1:
                break
            print('contents are update while compressing folder, retry %s' % attempt)
            time.sleep(1)

        return stdout, returncode

    def compress_git(self, path):
        if path.endswith('.git'):
            git_path = path
            parent = os.path.join(*os.path.split(path)[:-1])
            basename = os.path.basename(parent) + '.git.7z'
        else:
            git_path = os.path.join(path, '.git')
            basename = os.path.basename(path) + '.git.7z'

        if os.path.exists(git_path):
            zipfile = os.path.join('/tmp/', basename)
            self.compress(git_path, zipfile)
            return zipfile

def rotatenames(path):
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






if __name__ == '__main__':

    work = '/tmp/flowmonitor'
    backup = MegaBackup(('asterio.gonzalez@gmail.com'))

    zipfile = backup.compress_git(work)

    backup.start_daemon()

    backup.upload(zipfile, '/test/', rotate=True, remove_after=True)

    # ls = backup.execute('ls')
    # print(ls)
    # filename = os.path.abspath(__file__)
    # r = backup.execute('put', filename, '/test/')

    backup.upload_folder(work, '/test/')

    backup.stop_daemon()

    print('-End-')




