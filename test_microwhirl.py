import unittest
from microwhirl import *
import time
import random
import multiprocessing as mp

def test_worker1(whirl):
    whirl.put("testq", "test_value")

class ProcGen(WhirlProcess):
    def run(self):
        for i in range(5):
            self.whirl.put("testq", "test_value")

#classes for complex test
#put numbers from rng to queue
class Gen(WhirlProcess):
    def __init__(self, rng):
        WhirlProcess.__init__(self)
        self.rng = rng
    def run(self):
        for i in self.rng:
            self.whirl.put("procq", i)
            time.sleep(random.random()) #pause imitation

def complex_worker(whirl):
    try:
        v = whirl.get("procq")
    except QueueTimeout:
        return
    time.sleep(random.random()) #pause imitation
    whirl.put("saveq", v*v)

class Saver(WhirlProcess):
    def __init__(self):
        WhirlProcess.__init__(self)
        self.saveset = []
        self.keepwork = True
    def processSignals(self):
        try:
            signal = self.qInput.get(False, 0.001)
        except qq.Empty:
            return
        if signal == SOFTCLOSE:
            self.keepwork = False
    def run(self):
        while True:
            try:
                v = self.whirl.get("saveq")
                time.sleep(random.random())
                self.saveset.append(v)
            except QueueTimeout:
                if not self.keepwork: # queue is empty and getted signal to stop
                    break
            self.processSignals()
        self.qOutput.put(self.saveset)

class TestWhirl(unittest.TestCase):
    def testQ1(self):
        w = MicroWhirl()
        w.addQueue("q1")
        w.addQueue("q2")
        w.addQueue("q1")
        self.assertTrue("q1" in w.queues.qList)
        self.assertTrue("q2" in w.queues.qList)
        self.assertFalse("q3" in w.queues.qList)
        w.closeAllQueues()
    def testQ2(self):
        w = MicroWhirl()
        w.addQueue("q1", 1) #queue size 1
        w.put("q1", "test")
        try:
            w.put("q1", "test2")
            self.fail("i can't put in queue size 1")
        except QueueTimeout:
            pass
        #w.closeQueue("q1")
        w.closeAllQueues()
    def testQ3(self):
        w = MicroWhirl()
        w.addQueue("q1", 1) #queue size 1
        try:
            w.get("q1")
            self.fail("i can't get from queue")
        except QueueTimeout:
            pass
        #w.closeQueue("q1")
        w.closeAllQueues()
    def testPQ1(self):
        w = MicroWhirl(2) #timeout 2 sec
        w.addQueue("testq")
        w.addWorker(SimpleWorkerProcess(test_worker1), 'simple')
        w.closeAllWorkers()
        w.startAllWorkers()
        while w.checkAlive("simple"): pass # wait for done
        try:
            v = w.get("testq")
        except QueueTimeout:
            self.fail("worker finish, but queue is empty")
        #w.closeQueue("testq")
        w.closeAllQueues()
        self.assertEqual(v, "test_value")
    def testPQ2(self):
        w = MicroWhirl(2) #timeout 2 sec
        w.addQueue("testq")
        w.addWorker(ProcGen(), 'simple')
        w.closeAllWorkers()
        w.startAllWorkers()
        while w.checkAlive("simple"): pass # wait for done
        n = 0
        try:
            while True:
                v = w.get("testq")
                n += 1
        except QueueTimeout:
            pass
            #self.fail("worker finish, but queue is empty")
        #w.closeQueue("testq")
        w.closeAllQueues()
        self.assertEqual(n, 5)
        time.sleep(1)
    def testPQComplex(self):
        w = MicroWhirl(2) #timeout 2 sec
        w.addQueue("procq")
        w.addQueue("saveq")
        w.addWorker(Gen([1,2,3]), 'gen')
        w.addWorker(Gen([4,5,6]), 'gen')
        w.addWorker(SimpleWorkerProcess(complex_worker), 'proc')
        w.addWorker(SimpleWorkerProcess(complex_worker), 'proc')
        svr = Saver()
        w.addWorker(svr, 'save')
        w.closeWorkers('gen')
        w.startAllWorkers()
        while w.checkAlive("gen"): pass
        while w.queueSize("procq")>0: pass
        w.closeWorkers("proc")
        while w.checkAlive("proc"): pass
        while w.queueSize("saveq")>0: pass
        w.closeWorkers("save")
        v = svr.qOutput.get(True)
        for i in [1,2,3,4,5,6]:
            self.assertTrue((i*i) in v, "%d not in result %s" % (i*i,','.join(map(str,v))))
        w.closeAllWorkers()
        w.closeAllQueues()
        time.sleep(1)

if __name__ == '__main__':
    unittest.main()

