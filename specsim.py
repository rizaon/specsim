#!/usr/bin/python
from abc import ABCMeta, abstractmethod
import math, copy

NUMNODE = 6
NUMRACK = 3
NUMTASK = 2
NUMBLOCK = NUMTASK
NUMSTAGE = 0
NUMREPL = 2

EnableTaskSymmetry = True
EnableOffRackReplica = False

def getRackID(node):
  return node % NUMRACK

class Attempt(object):
  def __init__(self, datanode, mapnode):
    self.datanode = datanode
    self.mapnode = mapnode

  def clone(self):
    return Attempt(self.datanode,self.mapnode)


class Task(object):
  def __init__(self):
    self.attempts = []
    self.splitloc = []
    self.bitcodes = []
    self.splitbitmap = 0

  def addAttempt(self, att):
    self.attempts.append(att)

  def getNumAttempt(self):
    return len(self.attempts)

  def getBitmap(self, append):
    return self.getSplitBitmap() * ((2**4)**append) \
      + self.getTaskBitmap(append)

  def getTaskBitmap(self, append):
    code = 0
    mult = 1
    for i in xrange(0,len(self.bitcodes)):
      code = code * (2**4) + self.bitcodes[i]
      append -= 1
    while (append>0):
      code = code * (2**4)
      append -= 1
    return code

  def getSplitBitmap(self):
    return self.splitbitmap

  def clone(self):
    ctask = Task()
    for att in self.attempts:
      ctask.addAttempt(att.clone())
    ctask.bitcodes = list(self.bitcodes)
    ctask.splitloc = list(self.splitloc)
    ctask.splitbitmap = self.splitbitmap
    return ctask


class SimTopology(object):
  def __init__(self, failure):
    self.runstage = -2
    self.jobprogress = .0
    self.currentstate = 0
    self.count = 1

    self.tasks = []
    for i in xrange(0,NUMTASK):
      self.tasks.append(Task())

    self.badnode = failure[0]
    self.badrack = failure[1]

  def addAttempt(self,tid,att):
    task = self.tasks[tid]
    task.addAttempt(att)

    state = 0
    bdn = (att.datanode == self.badnode) and (att.datanode <> -1)
    bmp = (att.mapnode == self.badnode) and (att.mapnode <> -1)
    bdnr = (getRackID(att.datanode) == self.badrack) \
      and (att.datanode <> -1)
    bmpr = (getRackID(att.mapnode) == self.badrack) \
      and (att.mapnode <> -1)
    state = state * 2 + (1 if bdn else 0)
    state = state * 2 + (1 if bmp else 0)
    state = state * 2 + (1 if bdnr else 0)
    state = state * 2 + (1 if bmpr else 0)
    task.bitcodes.append(state)

  def clone(self):
    klon = SimTopology((self.badnode,self.badrack))
    klon.runstage = self.runstage
    klon.jobprogress = self.jobprogress
    klon.currentstate = self.currentstate
    klon.count = self.count

    klon.tasks = [task.clone() for task in self.tasks]

    return klon

  def setSplitLoc(self,blockid,repl):
    task = self.tasks[blockid]
    bits = [(x,(x == self.badnode)*2+(getRackID(x) == self.badrack)) \
      for x in repl]
    if EnableTaskSymmetry:
      bits = sorted(bits, key=lambda x:x[1])
    task.splitloc = list([a for (a,b) in bits])
    task.splitbitmap = reduce(lambda x,y:x*4+y[1], bits, 0)

  def updateCount(self):
    self.runstage += 1
    stage = self.runstage + 1

    if EnableTaskSymmetry:
      self.tasks = sorted(self.tasks, key=lambda x:x.getBitmap(stage))

    code = 0
    for i in xrange(0,len(self.tasks)):
      code = code * (2**(2*NUMREPL)) + self.tasks[i].getSplitBitmap()
    for i in xrange(0,len(self.tasks)):
      task = self.tasks[i]
      code = code * (2**(NUMTASK*stage)) + task.getTaskBitmap(stage)
    self.currentstate = code

    if (self.runstage >= 0):
      # calc job progress
      tp = 1.0/(len(self.tasks))
      self.jobprogress = \
        reduce(lambda x,y: x+(0 if self.isSlow(y) else tp), \
        [att.attempts[-1] for att in self.tasks], .0) 

  def getBitmap(self):
    return self.currentstate

  def isSlow(self,att):
    bdn = (att.datanode == self.badnode) and (att.datanode <> -1)
    bmp = (att.mapnode == self.badnode) and (att.mapnode <> -1)
    bdnr = (getRackID(att.datanode) == self.badrack) \
      and (att.datanode <> -1)
    bmpr = (getRackID(att.mapnode) == self.badrack) \
      and (att.mapnode <> -1)
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
    locatedDN = set(sim.tasks[tid].splitloc)

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
    locatedDN = sim.tasks[tid].splitloc
    for i in locatedDN:
      for j in xrange(0,NUMNODE):
        att = Attempt(i,j)
        if self.isAttemptAllowed(sim,tid,att):
          backups.append(att)
    return backups


def permuteBlocks(sim,blockid,repl):
  ret = []
  if blockid < NUMBLOCK:
    if len(repl) < NUMREPL:
      for i in xrange(0,NUMNODE):
        if not (i in repl):
          ret.extend(permuteBlocks(sim,blockid,repl + [i]))
    else:
      racks = set([getRackID(x) for x in repl])
      if not EnableOffRackReplica or (len(racks) > 1):
        psim = sim.clone()
        psim.setSplitLoc(blockid,repl)
        ret.extend(permuteBlocks(psim,blockid+1,[]))
  else:
    sim.updateCount()
    ret.append(sim)
  return ret

def permuteOriginal(sim,tid):
  ret = []
  if tid < NUMTASK:
    for i in sim.tasks[tid].splitloc:
      for j in xrange(0,NUMNODE):
        att = Attempt(i,j)
        psim = sim.clone()
        psim.addAttempt(tid,att)
        ret.extend(permuteOriginal(psim,tid+1))
    return ret
  else:
    sim.updateCount()
    ret.append(sim)
  return ret  

SPEC = BasicSE()

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
    sim.updateCount()
    ret.append(sim)
  return ret  

def getFormatBit(sim):
  stage = sim.runstage + 1
  taskBitLength = NUMSTAGE*4
  outstr = ""
  taskBits = []
  for task in sim.tasks:
    dnbit = ("{0:0" + str(2*NUMREPL) + "b}").format(task.getSplitBitmap())
    taskBits.append(dnbit)
  outstr += ",".join(taskBits)
  taskBits = []
  for task in sim.tasks:
    st = ("{0:0" + str(taskBitLength) + "b}").format(task.getTaskBitmap(stage))
    taskbit = ",".join([st[i:i+4] for i in xrange(0,len(st),4)])
    taskBits.append(taskbit)
  outstr += "-" + "|".join(taskBits)
  return outstr

def getTaskTopology(sim):
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

def isLimplock(sim):
  limp = False
  if sim.runstage >= 0:
    for i in xrange(0,NUMTASK):
      limp = limp or sim.isSlow(sim.tasks[i].attempts[-1])
  return limp

def printPerms(queue):
  uniquePerm = len(queue)
  uniqueSucc = reduce(lambda x,y: x+(1 if not isLimplock(y) else 0), \
    queue.values(),0)
  uniqueFail = reduce(lambda x,y: x+(1 if isLimplock(y) else 0), \
    queue.values(),0)
  totalPerm = reduce(lambda x,y: x+y.getCount(),queue.values(),0)
  totalSucc = reduce(lambda x,y: x+(y.getCount() if not isLimplock(y) else 0), \
    queue.values(),0)
  totalFail = reduce(lambda x,y: x+(y.getCount() if isLimplock(y) else 0), \
    queue.values(),0)

  print "Unique permutation: ", uniquePerm
  print "Unique success: ", uniqueSucc
  print "Unique failure: ", uniqueFail
  print "Total permutation: ", totalPerm
  print "Total success: ", totalSucc
  print "Total failure: ", totalFail
  print "Fail probability: ", totalFail/float(totalPerm) 
  print "====================================="
  for k,v in sorted(queue.items(), key=lambda x:x[0]):
    print "Hash key: ", k
    print "Hash bit: ", getFormatBit(v)
    print "Total count: ", v.getCount()
    print "Probability: ", v.getCount()/float(totalPerm)
    print "Bad node: ", v.badnode
    print "Bad rack: ", v.badrack
    print "Topology: ", getTaskTopology(v)
    print "Datanodes: ", [t.splitloc for t in v.tasks]
    print "IsLimplock:", isLimplock(v)
    print "====================================="


def main():
  failurequeue = []
  for i in xrange(0,NUMNODE):
    failurequeue.append(SimTopology((i,-1)))
#  for i in xrange(0,NUMRACK):
#    failurequeue.append(SimTopology((-1,i)))

  """ stage -1: permute datablocks  """
  if NUMSTAGE > -1:
    dnqueue = dict()
    while len(failurequeue) > 0:
      sim = failurequeue.pop(0)
      perms = permuteBlocks(sim, 0, [])
      while len(perms) > 0:
        nextsim = perms.pop(0)
        if nextsim.getBitmap() in dnqueue:
          sameperm = dnqueue[nextsim.getBitmap()]
          sameperm.count += nextsim.count
        else:
          dnqueue[nextsim.getBitmap()] = nextsim

  if NUMSTAGE == 0:
    printPerms(dnqueue)
    

  """ stage  0: permute original tasks """
  if NUMSTAGE > 0:
    blockperms = dnqueue.values()
    originalqueue = dict()
    while len(blockperms) > 0:
      sim = blockperms.pop(0)
      perms = permuteOriginal(sim, 0)
      while len(perms) > 0:
        nextsim = perms.pop(0)
        if nextsim.getBitmap() in originalqueue:
          sameperm = originalqueue[nextsim.getBitmap()]
          sameperm.count += nextsim.count
        else:
          originalqueue[nextsim.getBitmap()] = nextsim

  if NUMSTAGE == 1:
    printPerms(originalqueue)


  """ stage  1: run SE """
  if NUMSTAGE > 1:
    ori = originalqueue.values()
    finalStates = dict()
    while (len(ori) > 0):
      sim = ori.pop(0)
      perms = permuteStage(sim, 0)
      while len(perms) > 0:
        nextsim = perms.pop(0)
        if nextsim.getBitmap() in finalStates:
          sameperm = finalStates[nextsim.getBitmap()]
          sameperm.count += nextsim.count
        else:
          finalStates[nextsim.getBitmap()] = nextsim

  if NUMSTAGE == 2:
    printPerms(finalStates)

if __name__ == '__main__':
  main()

