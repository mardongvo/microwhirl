# -*- coding: utf-8 -*-
"""Controlable multiprocessing pool

A small module for parallelizing data processing using queues and processes.

Common case to use:
1) Create queues.
2) Create processes to generate jobs, process jobs, save results.
3) Start processes.
4) Wait for specific processes, process groups, or queues to finish.

No new queues are expected.

You can organize non-linear processing of tasks: return to the queue, transfer to different queues depending on the conditions, etc.

Sample processing graph:
| Generator 1 | -> | ======= | -> | Worker 1 | -|
| Generator 2 | -> | Queue 1 | -> | Worker 2 | -> | Queue to save  | -> | Saver process |
| Generator 3 | -> | ======= | -> | Worker 3 | -| | Queue for gens |
     ^                                                   |
     |----------------------------------------------------
"""

import multiprocessing as mp
import queue as qq

class MicroWhirlException(Exception):
    pass

class QueueNotExists(MicroWhirlException):
    pass

class QueueTimeout(MicroWhirlException):
    pass

#constant to signal process for soft closing
SOFTCLOSE = "softclose"

class WhirlProcess(mp.Process):
    """ Base class process
    It can create two queues (by default) to communicate with parent process
    """
    def __init__(self, whirl, needInput = True, needOutput = True):
        mp.Process.__init__(self)
        self.whirl = whirl
        self.qInput = None
        self.qOutput = None
        if needInput:
            self.qInput = mp.Queue()
        if needOutput:
            self.qOutput = mp.Queue()
    def processSignals(self): pass
    def run(self): pass


class SimpleWorkerProcess(WhirlProcess):
    """ Simple context-free worker
    """
    def __init__(self, whirl, worker_func):
        WhirlProcess.__init__(self, whirl, True, False)
        self.worker = worker_func
        self._softclose = False
    def processSignals(self):
        try:
            signal = self.qInput.get(False, 0.001)
        except qq.Empty:
            return
        if signal == SOFTCLOSE:
            self._softclose = True
    def run(self):
        while not self._softclose:
            self.processSignals()
            self.worker(self.whirl)

class MicroWhirlQueues:
    """ Pickable queue list to transfer to child processes
    """
    def __init__(self, qtimeout=1):
        self.qList = {}
        self.qTimeout = qtimeout #timeout to put in/get from queues
    def addQueue(self, qname, qsize=0):
        #add Queue by name, skip if exists
        if qname in self.qList: return
        self.qList[qname] = mp.Queue(qsize)
    def closeQueue(self, qname):
        #close Queue by name, skip if not exists
        if qname not in self.qList: return
        self.qList[qname].close()
    def closeAllQueues(self):
        for k,q in self.qList.items():
            q.close()
    def queueSize(self, qname):
        if qname not in self.qList: return 0
        return self.qList[qname].qsize()
    def put(self, qname, obj):
        #non-blocking try to put obj in queue
        if qname not in self.qList:
            raise QueueNotExists("Queue "+qname+" not exists")
        try:
            self.qList[qname].put(obj, False, self.qTimeout)
        except qq.Full:
            raise QueueTimeout("put: queue "+qname+" timeout")
    def get(self, qname):
        #non-blocking try to get obj in queue
        if qname not in self.qList:
            raise QueueNotExists("Queue "+qname+" not exists")
        try:
            return self.qList[qname].get(False, self.qTimeout)
        except qq.Empty:
            raise QueueTimeout("get: queue "+qname+" timeout")

class MicroWhirl:
    """ Controller for main process
    
    """
    def __init__(self, qtimeout=1):
        self.wList = [] #process obj, tag
        self.queues = MicroWhirlQueues(qtimeout)
    #MicroWhirlQueues reflect
    def addQueue(self, qname, qsize=0):
        self.queues.addQueue(qname, qsize)
    def closeQueue(self, qname):
        self.queues.closeQueue(qname)
    def closeAllQueues(self):
        self.queues.closeAllQueues()
    def queueSize(self, qname):
        return self.queues.queueSize(qname)
    def put(self, qname, obj):
        self.queues.put(qname, obj)
    def get(self, qname):
        return self.queues.get(qname)
    def addWorker(self, worker_obj, tag=''):
        """ Add process object to control
        """
        self.wList.append( (worker_obj, tag) )
    #start workers by tag
    def startWorkers(self, tag):
        """ Start workers by tag
        """
        for p, t in self.wList:
            if t == tag:
                p.start()
    def startAllWorkers(self):
        """ Start all workers
        """
        for p, t in self.wList:
            p.start()
    def closeWorkers(self, tag):
        """ Soft close workers by tag
        """
        for p, t in self.wList:
            if t == tag:
                try:
                    if p.qInput != None:
                        p.qInput.put(SOFTCLOSE, False, 0)
                except qq.Full:
                    pass
    def closeAllWorkers(self):
        """ Soft close all workers
        """
        for p, t in self.wList:
            try:
                if p.qInput != None:
                    p.qInput.put(SOFTCLOSE, False, 0)
            except qq.Full:
                pass
    def checkAlive(self, tag):
        """ Check alive processes by tag
            return False only if all processes are dead
        """
        alive = False
        for p, t in self.wList:
            if t == tag:
                alive = alive or p.is_alive()
        return alive
    def checkAllAlive(self):
        """ Check alive all processes
            return False only if all processes are dead
        """
        alive = False
        for p, t in self.wList:
            alive = alive or p.is_alive()
        return alive