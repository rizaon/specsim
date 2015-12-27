#!/usr/bin/python
from abc import ABCMeta, abstractmethod
import math, copy, time

NUMNODE = 6
NUMRACK = 3
NUMTASK = 2
NUMBLOCK = NUMTASK
NUMSTAGE = 0
NUMREPL = 2

EnableStateCollapsing = True
EnableTaskSymmetry = EnableStateCollapsing and True
EnableOffRackReplica = False

def getRackID(node):
  return node % NUMRACK

class TimeReporter(object):
  def __init__(self):
    self.begin = time.clock()
    self.end = time.clock()

  def start(self):
    self.begin = time.clock()

  def stop(self):
    self.end = time.clock()

  def getElapsed(self):
    return self.end - self.begin

  def report(self,msg,queue):
    print msg
    print "Time elapsed: %f " % self.getElapsed()
    print "Queue size: %d" % len(queue)
    print ""


class Attempt(object):
  def __init__(self, datanode, mapnode):
    self.datanode = datanode
    self.mapnode = mapnode

  def clone(self):
    return Attempt(self.datanode,self.mapnode)

class HdfsFile(object):
  def __init__(self, numblock, numrepl):
    self.numblock = numblock
    self.numrepl = numrepl
    self.blocks = [[-1]*numrepl for i in xrange(0,numblock)]

  def clone(self):
    klon = HdfsFile(self.numblock, self.numrepl)
    klon.blocks = copy.deepcopy(self.blocks)
    return klon

class Task(object):
  def __init__(self):
    self.attempts = []

  def addAttempt(self, att):
    self.attempts.append(att)

  def getNumAttempt(self):
    return len(self.attempts)

  def clone(self):
    ctask = Task()
    for att in self.attempts:
      ctask.addAttempt(att.clone())
    return ctask

class SimTopology(object):
  def __init__(self, failure):
    self.runstage = -1
    self.jobprogress = .0
    self.currentstate = 0
    self.count = 1

    self.file = HdfsFile(NUMBLOCK, NUMREPL)

    self.tasks = []
    for i in xrange(0,NUMTASK):
      self.tasks.append(Task())

    self.badnode = failure[0]
    self.badrack = failure[1]

  def addAttempt(self,tid,att):
    task = self.tasks[tid]
    task.addAttempt(att)

  def clone(self):
    klon = SimTopology((self.badnode,self.badrack))
    klon.runstage = self.runstage
    klon.jobprogress = self.jobprogress
    klon.currentstate = self.currentstate
    klon.count = self.count

    klon.file = self.file.clone()
    klon.tasks = [task.clone() for task in self.tasks]

    return klon

  def setBlocks(self,blockid,repl):
    self.file.blocks[blockid] = repl

  def updateProgress(self):
    self.runstage += 1
    stage = self.runstage + 1

    if (self.runstage >= 0):
      # calc job progress
      tp = 1.0/(len(self.tasks))
      self.jobprogress = \
        reduce(lambda x,y: x+(0 if self.isSlow(y) else tp), \
        [att.attempts[-1] for att in self.tasks], .0) 

  def isSlow(self,att):
    bdn = (att.datanode == self.badnode) and (self.badnode <> -1)
    bmp = (att.mapnode == self.badnode) and (self.badnode <> -1)
    bdnr = (getRackID(att.datanode) == self.badrack) \
      and (self.badrack <> -1)
    bmpr = (getRackID(att.mapnode) == self.badrack) \
      and (self.badrack <> -1)
    return ((not bdn and bmp) or (bdn and not bmp)) or \
      ((not bdnr and bmpr) or (bdnr and not bmpr))

  def needBackup(self,tid):
    if (self.tasks[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return  self.isSlow(self.tasks[tid].attempts[-1])  and \
        self.jobprogress > 0.2

  def getCount(self):
    return self.count


class Speculator:
  __metaclass__ = ABCMeta
  @abstractmethod
  def getPossibleBackups(self,sim,tid): pass

class BasicSE(Speculator):
  def isAttemptAllowed(self,sim,tid,att):
    dislike = [a.mapnode for a in sim.tasks[tid].attempts]
    locatedDN = set(sim.file.blocks[tid])

    differentWorknode = not (att.mapnode in dislike)
    canPickDatanode = (att.datanode in locatedDN)
#    canReadLocal = (att.mapnode in locatedDN)
#    canReadRackLocal = reduce(lambda x,y:x or (getRackID(y) == getRackID(att.mapnode)), \
#      locatedDN, False)
#    readingLocal = (att.datanode == att.mapnode)
#    readingRackLocal = (getRackID(att.datanode) == getRackID(att.mapnode))
    return differentWorknode and canPickDatanode

  def getPossibleBackups(self,sim,tid):
    backups = []
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,NUMNODE):
        att = Attempt(dn,map)
        if self.isAttemptAllowed(sim,tid,att):
          backups.append(att)
    return backups


class Bitcoder(object):
  def getNodeBitmap(self,sim,node):
    return (node == sim.badnode)*2 + (getRackID(node) == sim.badrack)

  def getBlockBitmap(self,sim,repl):
    return reduce(lambda x,y:x*4+self.getNodeBitmap(sim,y), repl , 0)

  def getFileBitmap(self,sim):
    blockbit = reduce(lambda x,y:x*(4**NUMREPL)+self.getBlockBitmap(sim,y), \
      sim.file.blocks, 0)
    return blockbit

  def getAttemptBitmap(self,sim,att):     
    state = 0
    bdn = (att.datanode == sim.badnode) and (sim.badnode <> -1)
    bmp = (att.mapnode == sim.badnode) and (sim.badnode <> -1)
    bdnr = (getRackID(att.datanode) == sim.badrack) \
      and (sim.badrack <> -1)
    bmpr = (getRackID(att.mapnode) == sim.badrack) \
      and (sim.badrack <> -1)
    state = state * 2 + bdn
    state = state * 2 + bmp
    state = state * 2 + bdnr
    state = state * 2 + bmpr
    return state
   
  def getTaskBitmap(self,sim,task):
    return reduce(lambda x,y: x*16 + \
      self.getAttemptBitmap(sim,y),task.attempts,0)

  def getTasksBitmap(self,sim):
    stage = sim.runstage + 1
    return reduce(lambda x,y: x*(16**stage) + \
      self.getTaskBitmap(sim,y)*(16**(stage-len(y.attempts))),sim.tasks,0)

  def getSimBitmap(self,sim):
    stage = sim.runstage + 1
    return self.getFileBitmap(sim) * (16**(len(sim.tasks)*stage)) + \
      self.getTasksBitmap(sim)

  def getFormattedSimBitmap(self,sim):
    taskBitLength = (sim.runstage+1)*4
    outstr = ""

    dnbit = ("{0:0" + str(2*NUMBLOCK*NUMREPL) + "b}").format(BC.getFileBitmap(sim))
    outstr += ",".join([dnbit[i:i+2*NUMREPL] for i in xrange(0,len(dnbit),2*NUMREPL)])

    taskBits = []
    for task in sim.tasks:
      st = ("{0:0" + str(taskBitLength) + "b}").format(BC.getTaskBitmap(sim,task))
      taskbit = ",".join([st[i:i+4] for i in xrange(0,len(st),4)])
      taskBits.append(taskbit)
    outstr += "-" + "|".join(taskBits)
    return outstr


class Optimizer(object):
  def __init__(self):
    self.bc = Bitcoder()

  def reorderBlocks(self,sim):
    for i in xrange(0,len(sim.file.blocks)):
      sim.file.blocks[i] = sorted(sim.file.blocks[i], \
        key=lambda x:(self.bc.getNodeBitmap(sim,x),x))

    sim.file.blocks = sorted(sim.file.blocks, \
        key=lambda x:(self.bc.getBlockBitmap(sim,x),x))


  def reorderTasks(self,sim):
    tuples = []
    stage = sim.runstage + 1
    for i in xrange(0,len(sim.tasks)):
      tuple = (sim.tasks[i],sim.file.blocks[i], \
        self.bc.getBlockBitmap(sim,sim.file.blocks[i]) * (16**stage) +\
        self.bc.getTaskBitmap(sim,sim.tasks[i]))
      tuples.append(tuple)
    tuples = sorted(tuples, key = lambda x:x[2])
    for i in xrange(0,len(sim.tasks)):
      sim.tasks[i] = tuples[i][0]
      sim.file.blocks[i] = tuples[i][1]


class Printer(object):
  def __init__(self):
    self.bc = Bitcoder()

  def getTaskTopology(self,sim):
    topo = []
    for i in xrange(0,len(sim.tasks)):
      task = sim.tasks[i]
      for j in xrange(0,len(task.attempts)):
        att = task.attempts[j]
        tuple = ("t%d_%d" % (i,j), att.datanode, att.mapnode)
        if (tuple[1] == -1) and (tuple[2] == -1):
          continue
        else:
          topo.append(tuple)
    return topo

  def isLimplock(self,sim):
    limp = False
    if sim.runstage >= 0:
      for i in xrange(0,NUMTASK):
        limp = limp or sim.isSlow(sim.tasks[i].attempts[-1])
    return limp

  def printPerms(self,queue):
    uniquePerm = len(queue)
    uniqueSucc = 0
    uniqueFail = 0
    totalPerm = 0
    totalSucc = 0
    totalFail = 0

    for v in queue:
      limp = self.isLimplock(v)
      uniqueSucc += not limp
      uniqueFail += limp
      totalPerm += v.getCount()
      totalSucc += v.getCount() if not limp else 0
      totalFail += v.getCount() if limp else 0

    print "Unique permutation: ", uniquePerm
    print "Unique success: ", uniqueSucc
    print "Unique failure: ", uniqueFail
    print "Total permutation: ", totalPerm
    print "Total success: ", totalSucc
    print "Total failure: ", totalFail
    print "Fail ratio: ", totalFail/float(totalPerm) 
    print "====================================="
    tuples = map(lambda x: (self.bc.getSimBitmap(x),x), queue)
    for k,v in sorted(tuples, key=lambda x:x[0]):
      print "Hash key: ", k
      if EnableStateCollapsing:
        print "Hash bit: ", self.bc.getFormattedSimBitmap(v)
      print "Total count: ", v.getCount()
      print "Ratio: ", v.getCount()/float(totalPerm)
      print "Bad node: ", v.badnode
      print "Bad rack: ", v.badrack
      print "Topology: ", self.getTaskTopology(v)
      print "Datanodes: ", v.file.blocks
      print "IsLimplock:", self.isLimplock(v)
      print "====================================="



SPEC = BasicSE()
BC = Bitcoder()
OPT = Optimizer()
TIME = TimeReporter()

def permuteFailure():
  TIME.start()

  failurequeue = []
  for i in xrange(0,NUMNODE):
    failurequeue.append(SimTopology((i,-1)))
#  for i in xrange(0,NUMRACK):
#    failurequeue.append(SimTopology((-1,i)))

  TIME.stop()
  TIME.report("Failure permutation complete!",failurequeue)
  return failurequeue

def placeBlock(queue, blockid):
  TIME.start()

  for i in xrange(0,NUMREPL):
    tmp = queue
    queue = []
    while len(tmp)>0:
      sim = tmp.pop(0)
      for j in xrange(0,NUMNODE):
        if j not in sim.file.blocks[blockid]:
          psim = sim.clone()
          psim.file.blocks[blockid][i] = j
          queue.append(psim)

  TIME.stop()
  TIME.report("Block %d permutation done!" % blockid,queue)
  return queue

def permuteBlock(queue):
  for i in xrange(0,NUMBLOCK):
    queue = placeBlock(queue,i)
  return queue

def reduceBlockPerms(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if EnableTaskSymmetry:
      OPT.reorderBlocks(sim)
    id = BC.getSimBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      sameperm.count += sim.count
    else:
      ret[id] = sim

  TIME.stop()
  TIME.report("Reduction of block permutation done!" ,queue)
  return ret.values()


def permuteOriginal(sim,tid):
  ret = []
  if tid < NUMTASK:
    for dn in sim.file.blocks[tid]:
      for map in xrange(0,NUMNODE):
        att = Attempt(dn,map)
        psim = sim.clone()
        psim.addAttempt(tid,att)
        ret.extend(permuteOriginal(psim,tid+1))
    return ret
  else:
    sim.updateProgress()
    if EnableTaskSymmetry:
      OPT.reorderTasks(sim)
    ret.append(sim)
  return ret  

def permuteStage(sim,tid):
  ret = []
  if tid < NUMTASK:
    if sim.needBackup(tid):
      backups = SPEC.getPossibleBackups(sim,tid)
      while len(backups) > 0:
        att = backups.pop(0)
        psim = sim.clone()
        psim.addAttempt(tid,att)
        ret.extend(permuteStage(psim,tid+1))
    else:
      ret.extend(permuteStage(sim,tid+1))
    return ret
  else:
    sim.updateProgress()
    if EnableTaskSymmetry:
      OPT.reorderTasks(sim)
    ret.append(sim)
  return ret  

def main():
  printer = Printer()

  failurequeue = permuteFailure()

  """ stage -1: permute datablocks  """
  dnqueue = permuteBlock(failurequeue)
  if EnableStateCollapsing:
    dnqueue = reduceBlockPerms(dnqueue)

  if NUMSTAGE == 0:
    printer.printPerms(dnqueue)
    

  """ stage  0: permute original tasks """
  if NUMSTAGE > 0:
    blockperms = dnqueue
    originalqueue = dict()
    id = 0
    while len(blockperms) > 0:
      sim = blockperms.pop(0)
      perms = permuteOriginal(sim, 0)
      while len(perms) > 0:
        nextsim = perms.pop(0)
        id = BC.getSimBitmap(nextsim) if EnableStateCollapsing else (id+1)
        if id in originalqueue:
          sameperm = originalqueue[id]
          sameperm.count += nextsim.count
        else:
          originalqueue[id] = nextsim

  if NUMSTAGE == 1:
    printer.printPerms(originalqueue.values())


  """ stage  1: run SE """
  if NUMSTAGE > 1:
    ori = originalqueue.values()
    finalStates = dict()
    id = 0
    while (len(ori) > 0):
      sim = ori.pop(0)
      perms = permuteStage(sim, 0)
      while len(perms) > 0:
        nextsim = perms.pop(0)
        id = BC.getSimBitmap(nextsim) if EnableStateCollapsing else (id+1)
        if id in finalStates:
          sameperm = finalStates[id]
          sameperm.count += nextsim.count
        else:
          finalStates[id] = nextsim

  if NUMSTAGE == 2:
    printer.printPerms(finalStates.values())

if __name__ == '__main__':
  main()

