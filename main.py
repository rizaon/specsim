#!/usr/bin/python
from abc import ABCMeta, abstractmethod
from enum import Enum
import math, copy, time


class Conf(object):
  def __init__(self):
    self.NUMNODE = 6
    self.NUMRACK = 3
    self.NUMTASK = 2
    self.NUMBLOCK = self.NUMTASK
    self.NUMSTAGE = 2
    self.NUMREPL = 2

    self.EnableStateCollapsing = True
    self.EnableTaskSymmetry = self.EnableStateCollapsing and True
    self.EnableOffRackReplica = False
    self.PrintPermutations = True

  def getRackID(self,node):
    return node % self.NUMRACK

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
  def __init__(self, conf, failure):
    self.conf = conf
    self.runstage = -1
    self.jobprogress = .0
    self.currentstate = 0
    self.count = 1

    self.file = HdfsFile(self.conf.NUMBLOCK, self.conf.NUMREPL)

    self.tasks = []
    for i in xrange(0,self.conf.NUMTASK):
      self.tasks.append(Task())

    self.badnode = failure[0]
    self.badrack = failure[1]

  def addAttempt(self,tid,att):
    task = self.tasks[tid]
    task.addAttempt(att)

  def clone(self):
    klon = SimTopology(self.conf,(self.badnode,self.badrack))
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
    bdnr = (self.conf.getRackID(att.datanode) == self.badrack) \
      and (self.badrack <> -1)
    bmpr = (self.conf.getRackID(att.mapnode) == self.badrack) \
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
  def __init__(self,conf):
    self.conf = conf

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
    dislike = [a.mapnode for a in sim.tasks[tid].attempts]
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,self.conf.NUMNODE):
        if map not in dislike:
          backups.append(Attempt(dn,map))
    return backups


class Bitcoder(object):
  def __init__(self,conf):
    self.conf = conf

  def getNodeBitmap(self,sim,node):
    return (node == sim.badnode)*2 + (self.conf.getRackID(node) == sim.badrack)

  def getBlockBitmap(self,sim,repl):
    return reduce(lambda x,y:x*4+self.getNodeBitmap(sim,y), repl , 0)

  def getFileBitmap(self,sim):
    blockbit = reduce(lambda x,y:x*(4**self.conf.NUMREPL)+self.getBlockBitmap(sim,y), \
      sim.file.blocks, 0)
    return blockbit

  def getAttemptBitmap(self,sim,att):     
    state = 0
    bdn = (att.datanode == sim.badnode) and (sim.badnode <> -1)
    bmp = (att.mapnode == sim.badnode) and (sim.badnode <> -1)
    bdnr = (self.conf.getRackID(att.datanode) == sim.badrack) \
      and (sim.badrack <> -1)
    bmpr = (self.conf.getRackID(att.mapnode) == sim.badrack) \
      and (sim.badrack <> -1)
    state = state * 2 + bdn
    state = state * 2 + bmp
    state = state * 2 + bdnr
    state = state * 2 + bmpr
    return state
   
  def getTaskBitmap(self,sim,task):
    stage = sim.runstage + 1
    return reduce(lambda x,y: x*16 + \
      self.getAttemptBitmap(sim,y),task.attempts,0) * 16**(stage-len(task.attempts))

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
    NUMBLOCK = self.conf.NUMBLOCK
    NUMREPL = self.conf.NUMREPL

    dnbit = ("{0:0" + str(2*NUMBLOCK*NUMREPL) + "b}").format(self.getFileBitmap(sim))
    outstr += ",".join([dnbit[i:i+2*NUMREPL] for i in xrange(0,len(dnbit),2*NUMREPL)])

    taskBits = []
    for task in sim.tasks:
      st = ("{0:0" + str(taskBitLength) + "b}").format(self.getTaskBitmap(sim,task))
      taskbit = ",".join([st[i:i+4] for i in xrange(0,len(st),4)])
      taskBits.append(taskbit)
    outstr += "-" + "|".join(taskBits)
    return outstr


class Optimizer(object):
  def __init__(self,conf):
    self.bc = Bitcoder(conf)
    self.conf = conf

  def reorderBlocks(self,sim):
    for i in xrange(0,len(sim.file.blocks)):
      sim.file.blocks[i] = sorted(sim.file.blocks[i], \
        key=lambda x:(self.bc.getNodeBitmap(sim,x),x))

    sim.file.blocks = sorted(sim.file.blocks, \
        key=lambda x:(self.bc.getBlockBitmap(sim,x),x))


  def reorderTasks(self,sim, ignoreFileBitmap = False):
    tuples = []
    stage = sim.runstage + 1
    for i in xrange(0,len(sim.tasks)):
      code = (0 if ignoreFileBitmap \
        else self.bc.getBlockBitmap(sim,sim.file.blocks[i])) * (16**stage) + \
          self.bc.getTaskBitmap(sim,sim.tasks[i])
      tuple = (code, \
        sim.file.blocks[i], \
        sim.tasks[i])
      tuples.append(tuple)
    tuples = sorted(tuples, key = lambda x:x[0:2])
    for i in xrange(0,len(sim.tasks)):
      sim.file.blocks[i] = tuples[i][1]
      sim.tasks[i] = tuples[i][2]


class PermType(Enum):
  NORMAL = 0
  OB_DD = 1
  OB_DW = 2
  OB_WD = 3
  OB_WW = 4
  FS_DD = 5
  FS_DW = 6
  FS_WW = 7
  UNK = 8

class PermTypeChecker(object):
  def __init__(self,conf):
    self.bc = Bitcoder(conf)
    self.conf = conf

  def getLimpTaskIDs(self,sim):
    limp = []
    if sim.runstage >= 0:
      for i in xrange(0,self.conf.NUMTASK):
        if sim.isSlow(sim.tasks[i].attempts[-1]):
          limp.append(i)
    return limp

  def hasBadDatasource(self,topo,att):
    bdn = (att.datanode == topo.badnode) and (topo.badnode <> -1)
    bdnr = (self.conf.getRackID(att.datanode) == topo.badrack) \
      and (topo.badrack <> -1)
    return bdn or bdnr

  def hasBadWorker(self,topo,att):
    bmp = (att.mapnode == topo.badnode) and (topo.badnode <> -1)
    bmpr = (self.conf.getRackID(att.mapnode) == topo.badrack) \
      and (sim.badrack <> -1)
    return bmp or bmpr

  def checkPermType(self,topo):
    """ This check assume there are only 2 task """
    if len(topo.tasks)<>2:
      raise Exception("Topology has less/more than 2 tasks")
    tids = self.getLimpTaskIDs(topo)
    if not tids:
      return PermType.NORMAL
    elif len(tids) == len(topo.tasks):
      # FS type
      verd = [self.hasBadDatasource(topo,topo.tasks[x].attempts[0]) for x in tids]
      verd = sorted(verd)
      print verd
      if verd[0]:
        if verd[1]:
          return PermType.FS_DD
      else:
        if verd[1]:
          return PermType.FS_DW
        else:
          return PermType.FS_WW
    else:
      # OB type
      verd = [self.hasBadDatasource(topo,x) for x in topo.tasks[tids[0]].attempts]
      if verd[0]:
        if verd[1]:
          return PermType.OB_DD
        else:
          return PermType.OB_DW
      else:
        if verd[1]:
          return PermType.OB_WD
        else:
          return PermType.OB_WW

    # something wrong if returning here
    return PermType.UNK


class Printer(object):
  def __init__(self, conf):
    self.bc = Bitcoder(conf)
    self.ck = PermTypeChecker(conf)
    self.conf = conf

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
      for i in xrange(0,self.conf.NUMTASK):
        limp = limp or sim.isSlow(sim.tasks[i].attempts[-1])
    return limp

  def printSim(self,sim):
    print (sim.badnode,sim.badrack)
    print sim.file.blocks
    for t in sim.tasks:
      print [(x.datanode,x.mapnode) for x in t.attempts]

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
    if self.conf.PrintPermutations:
      tuples = map(lambda x: ((self.bc.getTasksBitmap(x),self.bc.getFileBitmap(x)),x), queue)
      for k,v in sorted(tuples, key=lambda x:x[0]):
        print "Hash key: ", k
        if self.conf.EnableStateCollapsing:
          print "Hash bit: ", self.bc.getFormattedSimBitmap(v)
        print "Total count: ", v.getCount()
        print "Ratio: ", v.getCount()/float(totalPerm)
        print "Bad node: ", v.badnode
        print "Bad rack: ", v.badrack
        print "Topology: ", self.getTaskTopology(v)
        print "Datanodes: ", v.file.blocks
        print "IsLimplock:", self.isLimplock(v)
        print "PermType:", self.ck.checkPermType(v)
        print "====================================="


CONF = Conf()
SPEC = BasicSE(CONF)
BC = Bitcoder(CONF)
OPT = Optimizer(CONF)
PRINT = Printer(CONF)
TIME = TimeReporter()

def permuteFailure():
  TIME.start()

  failurequeue = []
  for i in xrange(0,CONF.NUMNODE):
    failurequeue.append(SimTopology(CONF,(i,-1)))
  for i in xrange(0,CONF.NUMRACK):
    failurequeue.append(SimTopology(CONF,(-1,i)))

  TIME.stop()
  TIME.report("Failure permutation complete!",failurequeue)
  return failurequeue

def placeBlock(queue, blockid):
  TIME.start()

  blocks = [[x] for x in xrange(0,CONF.NUMNODE)]
  for rep in xrange(1,CONF.NUMREPL):
    tmp = blocks
    blocks = []
    for blk in tmp:
      for i in xrange(0,CONF.NUMNODE):
        if i not in blk:
          blocks.append(blk+[i])

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    for blk in blocks:
      psim = sim.clone()
      psim.file.blocks[blockid] = blk
      queue.append(psim)

  TIME.stop()
  TIME.report("Block %d permutation done!" % blockid,queue)
  return queue

def permuteBlock(queue):
  for i in xrange(0,CONF.NUMBLOCK):
    queue = placeBlock(queue,i)
  return queue

def reduceBlockPerms(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderBlocks(sim)
    id = BC.getSimBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      sameperm.count += sim.count
    else:
      ret[id] = sim
  ret = ret.values()

  TIME.stop()
  TIME.report("Reduction of block permutation done!" ,ret)
  return ret


def placeOriginalTask(queue,taskid):
  TIME.start()

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    attempts = []
    for i in sim.file.blocks[taskid]:
      for j in xrange(0,CONF.NUMNODE):
        attempts.append(Attempt(i,j))
    for att in attempts:
      psim = sim.clone()
      psim.addAttempt(taskid,att)
      queue.append(psim)

  TIME.stop()
  TIME.report("Task %d permutation done!" % taskid,queue)
  return queue

def permuteOriginalTask(queue):
  for i in xrange(0,CONF.NUMTASK):
    queue = placeOriginalTask(queue,i)
  for sim in queue:
    sim.updateProgress()
  return queue

def reduceTaskPerms(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderTasks(sim)
    id = BC.getSimBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      sameperm.count += sim.count
    else:
      ret[id] = sim
  ret = ret.values()

  TIME.stop()
  TIME.report("Reduction of block permutation done!" ,ret)
  return ret


def placeBackupTask(queue,taskid):
  TIME.start()
  nobackup = []

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    if not sim.needBackup(taskid):
      nobackup.append(sim)
    else:
      attempts = SPEC.getPossibleBackups(sim,taskid)
      for att in attempts:
        psim = sim.clone()
        psim.addAttempt(taskid,att)
        queue.append(psim)

  queue = nobackup + queue
  TIME.stop()
  TIME.report("Backup task %d permutation done!" % taskid,queue)
  return queue

def permuteBackupTask(queue):
  for i in xrange(0,CONF.NUMTASK):
    queue = placeBackupTask(queue,i)
  for sim in queue:
    sim.updateProgress()
  return queue


def reduceByTasksBitmap(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderTasks(sim, True)
    id = BC.getTasksBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      if BC.getFileBitmap(sameperm) < BC.getFileBitmap(sim):
        sim.count += sameperm.count
        ret[id] = sim
      else:
        sameperm.count += sim.count
    else:
      ret[id] = sim
  ret = ret.values()

  TIME.stop()
  TIME.report("Reduction by task bitmap done!" ,ret)
  return ret


def main():
  timer = TimeReporter()
  timer.start()
  simqueue = permuteFailure()

  """ stage -1: permute datablocks  """
  if CONF.NUMSTAGE > -1:
    simqueue = permuteBlock(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceBlockPerms(simqueue)
    timer.stop()
    timer.report("Up to block placement",simqueue)

    if CONF.NUMSTAGE == 0:
      PRINT.printPerms(simqueue)
      exit(0)    

  """ stage  0: permute original tasks """
  if CONF.NUMSTAGE > 0:
    simqueue = permuteOriginalTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue)
    timer.stop()
    timer.report("Up to task placement",simqueue)

    if CONF.NUMSTAGE == 1:
      PRINT.printPerms(simqueue)
      exit(0)

  """ stage  1: run SE """
  numbackup = 1
  if numbackup < CONF.NUMSTAGE:
    simqueue = permuteBackupTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue)
    timer.stop()
    timer.report("Up to %dth backup placement" % numbackup,simqueue)
    numbackup += 1


  if CONF.EnableStateCollapsing:
    simqueue = reduceByTasksBitmap(simqueue)
  PRINT.printPerms(simqueue)

if __name__ == '__main__':
  main()

