import os
import re
import subprocess
import sys
import threading
from time import time, sleep

from mininet.net import Mininet
from mininet.cli import CLI


def start(mn, *procs):
    map(lambda p: p.init(), procs)
    map(lambda p: p.start(), procs)

def stop(*procs):
    map(lambda p: p.stop(), procs)

class HostRole(object):
    def __init__(self, host):
        self.procStr = None
        self.proc = None
        self.host = host
        self.stdout = "/tmp/{0}-{1}.log".format(host.IP(),
                                                self.__class__.__name__)
        self.stderr = "/tmp/{0}-{1}.err".format(host.IP(),
                                                self.__class__.__name__)
    def init(self):
        pass

    def start(self):
        self.proc = self.host.popen(self.procStr,
                                    stdout=open(self.stdout, "wb"),
                                    stderr=open(self.stderr, "wb"))
    def stop(self):
        # subclasses can call proc.wait(), so proc may have already terminated
        try:
            self.proc.terminate()
        except Exception:
            pass

    def IP(self):
        return self.host.IP()

    def __repr__(self):
        return self.__str__()

class EmptyRole(HostRole):
    def __init__(self, host, name="Empty"):
        super(EmptyRole, self).__init__(host)
        self.name = name

    def init(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def __str__(self):
        return self.name

class MemcacheServer(HostRole):
    def __init__(self, host):
        super(MemcacheServer, self).__init__(host)
        self.procStr = "memcached -u nobody"

    def __str__(self):
        return "Memcached Server"

class RepGetClient(HostRole):
    def __init__(self, host, srvs, trials=-1, activeReps=None):
        super(RepGetClient, self).__init__(host)
        self.trials = trials
        self.procStr = None
        self.cont = True
        self.srvs = srvs
        self.host = host
        self.lock = threading.Lock()
        if activeReps is None:
            self.activeReps = len(self.srvs)
        else:
            self.activeReps = activeReps

    def start(self):
        thread = threading.Thread(target=self.threadStart)
        thread.start()

    def stop(self):
        self.cont = False

    def execPhp(self, code):
        proc = self.host.popen(['php'],
                            stdout=subprocess.PIPE,
                            stdin=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            close_fds=True)
        output = proc.communicate(code)[0]
        try:
            os.kill(proc.pid, signal.SIGTERM)
        except:
            pass
        return output

    def setActiveReps(self, num):
        self.lock.acquire()
        try:
            self.activeReps = num
            print "{0}: using {1} replicated servers".format(self.host.name,
                                                             self.activeReps)
        finally:
            self.lock.release()

    def threadStart(self):
        rettimes = []

        print "{0} using {1} replicas".format(self.host.name,
                                              self.activeReps)

        iters = 0
        while self.cont and (self.trials == -1 or iters < self.trials):
            iters += 1

            self.lock.acquire()
            try:
                active = self.srvs[:self.activeReps]
            finally:
                self.lock.release()

            threadtimes = [None] * len(active)
            threads = [None] * len(active)
            if True:
                for i, h in enumerate(active):
                    threads[i] = threading.Thread(target=self.mcget,
                                                  args=(h, threadtimes, i))
                    threads[i].start()

                for i in range(len(threads)):
                    threads[i].join()

#                print threadtimes
                #minidc.stats.mcStats.add(self.host.name, round(max(threadtimes) * 1000, 3))
                print self.host.name + " get response  "  +str(round(max(threadtimes) *
                    1000, 3))
                # save some CPU cycles, throttle memcache requests
                sleep(0.2)

    def mcget(self, srv, result, index):
        code = "<?php $mem = new Memcached();\n"
        code += "$mem->addServer(\"" + srv + "\", 11211);\n"
        code += """$time_start = microtime(true);
        $result = $mem->get("blah");
        if ($result) {
           //echo "Item retrieved from memcached";
        } else {
           //echo "No matching key, adding";
           $mem->set("blah", "blah", 3600) or die("Couldn't save to mc");
        }

        $time_end = microtime(true);
        $time = $time_end - $time_start;
        //$time = round($time * 1000, 3);
        echo "\r\n$time";
        ?>"""

        res = self.execPhp(code)
        try:
            elapsed = float(res)
        except:
            elapsed = -1
        result[index] = elapsed
        #print elapsed

    def __str__(self):
        return "Replicated Memcached Client"
