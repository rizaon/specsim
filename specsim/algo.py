

from abc import ABCMeta, abstractmethod
from .util import Bitcoder
from .mapred import *
import math

class Speculator:
  __metaclass__ = ABCMeta
  @abstractmethod
  def getPossibleBackups(self,sim,tid): pass


class BasicSE(Speculator):
  def __init__(self,conf):
    self.conf = conf

  def needBackup(self,sim,tid):
    if (sim.tasks[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getJobProg() - sim.getTaskProg(tid) > 0.2

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


class FAReadSE(Speculator):
  def __init__(self,conf):
    self.conf = conf

  def needBackup(self,sim,tid):
    if (sim.tasks[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getJobProg() - sim.getTaskProg(tid) > 0.2

  def isAttemptAllowed(self,sim,tid,att):
    dislike = [a.mapnode for a in sim.tasks[tid].attempts]
    read = [a.datanode for a in sim.tasks[tid].attempts]
    locatedDN = set(sim.file.blocks[tid])

    differentWorknode = not (att.mapnode in dislike)
    canPickDatanode = (att.datanode in locatedDN)
    differentDN = (att.datanode not in read)
    return differentWorknode and canPickDatanode and differentDN

  def getPossibleBackups(self,sim,tid):
    backups = []
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,self.conf.NUMNODE):
        att = Attempt(dn,map)
        if self.isAttemptAllowed(sim,tid,att):
          backups.append(att)
    return backups


class PathSE(Speculator):
  def __init__(self,conf):
    self.conf = conf
#    self.bc = Bitcoder(conf)
    self.HARDPREF = False

  def getPathGroup(self,task):
    att = task.attempts[-1]
    rD = self.conf.getRackID(att.datanode)
    rM = self.conf.getRackID(att.mapnode)
    if rD<rM:
      return (rD,rM)
    else:
      return (rM,rD)

  def getPathGroups(self,sim):
    d = {}
    for tid in range(0,len(sim.tasks)):
      key = self.getPathGroup(sim.tasks[tid])
      if key not in d:
        d[key] = (sim.getTaskProg(tid),[tid])
      else:
        (prog,tasks) = d[key]
        prog = (prog*len(tasks)+sim.getTaskProg(tid))/(len(tasks)+1)
        tasks.append(tid)
        d[key] = (prog,tasks)
    return d

  def hasBasicSpec(self,sim):
    for tid in range(0,len(sim.tasks)):
      if self.needBackup(sim,tid):
        return True
    return False

  def specTask(self,queue,tid):
    tmp = queue
    queue = []

    while len(tmp)>0:
      sim = tmp.pop()
      attempts = self.getPossibleBackups(sim,tid)
      for att in attempts:
        psim = sim.clone()
        psim.addAttempt(tid,att)
        psim.prob /= len(attempts)
        queue.append(psim)
    return queue

  def specPathGroup(self,queue):
    tmp = queue
    queue = []
    haveSpec = []

    while len(tmp)>0:
      sim = tmp.pop(0)
      if (sim.getJobProg()>=1.0) or (self.hasBasicSpec(sim)):
        haveSpec.append(sim)
      else:
        """ do path group spec here """
        groups = self.getPathGroups(sim)
        tospec = [sim]
        if len(groups)==1:
          """ SPOF, speculate one of the task """
          tid = len(sim.tasks)-1
          tospec = self.specTask(tospec,tid)
        else:
          """ speculate slowest group """
          gAvgProg = .0
          for k,(prog,tids) in groups.items():
            gAvgProg += prog/len(groups)
          for k,(prog,tids) in groups.items():
            if gAvgProg-prog > 0.2:
              for tid in tids:
                tospec = self.specTask(tospec,tid)
        queue.extend(tospec)

    return (queue,haveSpec)

  def needBackup(self,sim,tid):
    if (sim.tasks[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getJobProg() - sim.getTaskProg(tid) > 0.2

  def toPointDict(self,lst):
    d = {}
    for a in lst:
      if a not in d:
        d[a] = 1
      else:
        d[a] += 1
    return d

  def getNegPref(self,att,triedMN,triedDN,triedRR):
    mRack = self.conf.getRackID(att.mapnode)
    dRack = self.conf.getRackID(att.datanode)

    retryMN = triedMN.get(att.mapnode,0)
    retryDN = triedDN.get(att.datanode,0)
    pointMR  = triedRR.get(mRack,0)
    pointDR  = triedRR.get(dRack,0)

    pref = 0
    pref = pref*10 + pointMR + pointDR
    pref = pref*10 + retryMN + retryDN
    return pref

  def debug_PrintPoint(self,sim,task,attID):
    atts = task.attempts[:attID]
    triedMN = self.toPointDict([a.mapnode for a in atts])
    triedDN = self.toPointDict([a.datanode for a in atts])
    triedRR = self.toPointDict([self.conf.getRackID(a.datanode) for a in atts] + \
      [self.conf.getRackID(a.mapnode) for a in atts])
    print triedMN
    print triedDN
    print triedRR
    print self.getNegPref(task.attempts[attID],triedMN, triedDN,triedRR)

  def getPossibleBackups(self,sim,tid):
    atts = sim.tasks[tid].attempts
    triedMN = self.toPointDict([a.mapnode for a in atts])
    triedDN = self.toPointDict([a.datanode for a in atts])
    triedRR = self.toPointDict([self.conf.getRackID(a.datanode) for a in atts] + \
      [self.conf.getRackID(a.mapnode) for a in atts])

    backups = []
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,self.conf.NUMNODE):
        if map not in triedMN:
          att = Attempt(dn,map)
          pref =  self.getNegPref(att,triedMN, triedDN,triedRR)
          backups.append((pref,att))
    backups = sorted(backups)
    if self.HARDPREF:
      lowest = backups[0][0]
      lowpref = lambda (x,y): x <= lowest
      backups = filter(lowpref,backups)
    return [y for (x,y) in backups]


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

