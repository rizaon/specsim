

from abc import ABCMeta, abstractmethod
from .util import Bitcoder, Printer
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
    if (sim.getMapTasks()[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getMapTaskProg(tid) < 1.0

  def isAttemptAllowed(self,sim,tid,att):
    assert isinstance(att, MapAttempt)

    dislike = [a.mapnode for a in sim.getMapTasks()[tid].attempts]
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
    dislike = [a.mapnode for a in sim.getMapTasks()[tid].attempts]
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,self.conf.NUMNODE):
        if map not in dislike:
          backups.append(MapAttempt(dn,map))
    return backups


class FAReadSE(Speculator):
  def __init__(self,conf):
    self.conf = conf

  def needBackup(self,sim,tid):
    if (sim.getMapTasks()[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getMapTaskProg(tid) < 1.0

  def isAttemptAllowed(self,sim,tid,att):
    assert isinstance(att, MapAttempt)

    dislike = [a.mapnode for a in sim.getMapTasks()[tid].attempts]
    read = [a.datanode for a in sim.getMapTasks()[tid].attempts]
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
        att = MapAttempt(dn,map)
        if self.isAttemptAllowed(sim,tid,att):
          backups.append(att)
    return backups


class PathSE(Speculator):
  def __init__(self,conf):
    self.conf = conf
    self.bc = Bitcoder(conf)
    self.pr = Printer(conf)
    self.HARDPREF = True
    self.delayed = []

  def filterForDelay(self,queue):
    from collections import defaultdict
    tmp = queue
    queue = []

    while len(tmp)>0:
      sim = tmp.pop()
      hasNodeLocal = False
      rsc = defaultdict(int)
      nsc = defaultdict(int)
      (a,b) = (-1,-1)
      (c,d) = (-1,-1)
      for task in sim.getMapTasks():
        """ Rack level """
        (a,b) = self.getPathGroup(task)
        rsc[a] += 1
        if a!=b:
          rsc[b] += 1

        """ Node level"""
        att = task.attempts[-1]
        if (att.datanode < att.mapnode):
          (c,d) = (att.datanode,att.mapnode)
        else:
          (c,d) = (att.mapnode,att.datanode)
        hasNodeLocal = hasNodeLocal or (c==d)
        nsc[c] += 1
        if (c!=d):
          nsc[d] += 1

      mtasksize = len(sim.getMapTasks())
      spof = (rsc[a]==mtasksize) or \
          (rsc[b]==mtasksize) or \
          (nsc[c]==mtasksize) or \
          (nsc[d]==mtasksize)
      if not hasNodeLocal and spof:
        self.delayed.append(sim)
      else:
        queue.append(sim)

    return queue

  def getPathGroup(self,task):
    att = task.attempts[-1]
    rD = self.conf.getRackID(att.datanode)
    rM = self.conf.getRackID(att.mapnode)
    if rD<rM:
      return (rD,rM)
    else:
      return (rM,rD)

  """def getPathGroups(self,sim):
    d = {}
    mapTasks = sim.getMapTasks()
    for tid in range(0,len(mapTasks)):
      key = self.getPathGroup(mapTasks[tid])
      if key not in d:
        d[key] = (sim.getMapTaskProg(tid),[tid])
      else:
        (prog,tasks) = d[key]
        prog = (prog*len(tasks)+sim.getMapTaskProg(tid))/(len(tasks)+1)
        tasks.append(tid)
        d[key] = (prog,tasks)
    return d"""

  """def hasBasicSpec(self,sim):
    for tid in range(0,len(sim.getMapTasks())):
      if self.needBackup(sim,tid):
        return True
    return False"""

  """def specTask(self,queue,tid):
    tmp = queue
    queue = []

    while len(tmp)>0:
      sim = tmp.pop()
      attempts = self.getPossibleBackups(sim,tid)
      for att in attempts:
        psim = sim.clone()
        psim.addAttempt(tid,att,True)
        psim.prob /= len(attempts)
        queue.append(psim)
    return queue"""

  """def specPathGroup(self,queue):
    tmp = queue
    queue = []
    haveSpec = []

    while len(tmp)>0:
      sim = tmp.pop(0)
      if (sim.getMapProg()>=1.0) or (self.hasBasicSpec(sim)):
        haveSpec.append(sim)
      else:
        # do path group spec here
        groups = self.getPathGroups(sim)
        tospec = [sim]

        gAvgProg = .0
        slowestProg = 1.0
        for k,(prog,tids) in groups.items():
          gAvgProg += prog/len(groups)
          if prog<slowestProg:
            slowestProg = prog

        if gAvgProg-slowestProg <= 0.2:
          #no group pass threshold, spec one for each slowest group
          #print "Single group, single task spec"
          for k,(prog,tids) in groups.items():
            if prog == slowestProg:
              for tid in tids:
                if sim.getMapTaskProg(tid)<1.0:
                  tospec = self.specTask(tospec,tid)
                  break
        else:
          #speculate slowest group
          #print "All group spec"
          for k,(prog,tids) in groups.items():
            if gAvgProg-prog > 0.2:
              for tid in tids:
                if sim.getMapTaskProg(tid)<1.0:
                  tospec = self.specTask(tospec,tid)

        queue.extend(tospec)

    return (queue,haveSpec)"""

  def needBackup(self,sim,tid):
    if (sim.getMapTasks()[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getMapTaskProg(tid) < 1.0

  def needReduceBackup(self,sim,tid):
    if (sim.getReduceTasks()[tid].attempts == []):
      # no attempt ever scheduled, run original task
      return True
    else:
      return sim.getReduceTaskProg(tid) < 1.0

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

  def getReduceNegPref(self,att,triedRN,triedRR):
    rRack = self.conf.getRackID(att.reducenode)

    retryRN = triedRN.get(att.reducenode,0)
    pointRR  = triedRR.get(rRack,0)

    pref = 0
    pref = pref*10 + pointRR
    pref = pref*10 + retryRN
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

  def getTaskPrefScores(self,sim,tid):
    atts = sim.getMapTasks()[tid].attempts
    triedMN = self.toPointDict([a.mapnode for a in atts])
    triedDN = self.toPointDict([a.datanode for a in atts])
    triedRR = self.toPointDict([self.conf.getRackID(a.datanode) for a in atts] + \
      [self.conf.getRackID(a.mapnode) for a in atts])
    return (triedMN,triedDN,triedRR)

  def getJobPrefScores(self,sim,tid):
    """ TODO: good inter-rack path should have score 0 """
    mapnodes = []
    datanodes = []
    for task in sim.getMapTasks():
      for att in task.attempts:
        if sim.isMapSlow(att):
          mapnodes.append(att.mapnode)
          datanodes.append(att.datanode)

    triedMN = self.toPointDict(mapnodes)
    triedDN = self.toPointDict(datanodes)
    triedRR = self.toPointDict([self.conf.getRackID(a) for a in mapnodes] + \
      [self.conf.getRackID(a) for a in datanodes])
    return (triedMN,triedDN,triedRR)

  def getPossibleBackups(self,sim,tid):
    triedMN = [a.mapnode for a in (sim.getMapTasks()[tid].attempts)]

    # get preference scores from task attempts history
    # (scMN,scDN,scRR) = self.getTaskPrefScores(sim,tid)

    # get preference scores from all task attempts history in jobs
    (scMN,scDN,scRR) = self.getJobPrefScores(sim,tid)

    backups = []
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,self.conf.NUMNODE):
        if map not in triedMN:
          att = MapAttempt(dn,map)
          pref =  self.getNegPref(att,scMN, scDN,scRR)
          backups.append((pref,att))
    backups = sorted(backups)
    if self.HARDPREF:
      lowest = backups[0][0]
      lowpref = lambda (x,y): x <= lowest
      backups = filter(lowpref,backups)
    return [y for (x,y) in backups]

  def getPossibleReduceBackups(self,sim,tid):
    triedRN = [a.reducenode for a in (sim.getReduceTasks()[tid].attempts)]

    # get preference scores from task attempts history
    # (scMN,scDN,scRR) = self.getTaskPrefScores(sim,tid)
    # scRN = self.toPointDict(triedRN)

    # get preference scores from all task attempts history in jobs
    (scMN,scDN,scRR) = self.getJobPrefScores(sim,tid)
    slowRN = []
    for task in self.getReduceTasks():
      for att in task.attempts:
        if sim.isReduceSlow(att):
          slowRN.append(att.reducenode)
    scRN = self.toPointDict(slowRN)

    for red in xrange(0,self.conf.NUMNODE):
      if red not in triedRN:
        att = ReduceAttempt(red)
        pref =  self.getReduceNegPref(att,scRN,scRR)
        backups.append((pref,att))
    backups = sorted(backups)
    if self.HARDPREF:
      lowest = backups[0][0]
      lowpref = lambda (x,y): x <= lowest
      backups = filter(lowpref,backups)
    return [y for (x,y) in backups]

  def printPathSEStat(self):
    ct = len(self.delayed)
    delfract = .0
    for sim in self.delayed:
      delfract += sim.prob
    print "Delayed count: ", ct
    print "Delayed fract: ", delfract


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

  def reorderBlocksPartial(self,sim,size):
    blk = len(sim.file.blocks)
    pad = []
    if size < blk:
      pad = sim.file.blocks[size:blk]
      sim.file.blocks = sim.file.blocks[0:size]

    self.reorderBlocks(sim)

    if pad:
      sim.file.blocks = sim.file.blocks + pad

  def reorderTasks(self,sim,ignoreFileBitmap = False):
    tuples = []
    mapbitset = self.conf.NUMMAP

    for i in xrange(0,len(sim.mapTasks)):
      code = (0 if ignoreFileBitmap \
        else self.bc.getBlockBitmap(sim,sim.file.blocks[i])) * (16**mapbitset) + \
          self.bc.getMapTaskBitmap(sim,sim.mapTasks[i])
      tuple = (code, \
        sim.file.blocks[i], \
        sim.mapTasks[i])
      tuples.append(tuple)

    tuples = sorted(tuples, key = lambda x:x[0:2])
    for i in xrange(0,len(sim.mapTasks)):
      sim.file.blocks[i] = tuples[i][1]
      sim.mapTasks[i] = tuples[i][2]

  def reorderTasksPartial(self,sim,size):
    blk = len(sim.file.blocks)
    blkpad = []
    tskpad = []
    if size < blk:
      blkpad = sim.file.blocks[size:blk]
      sim.file.blocks = sim.file.blocks[0:size]
      tskpad = sim.mapTasks[size:blk]
      sim.mapTasks = sim.mapTasks[0:size]

    self.reorderTasks(sim,False)

    if blkpad:
      sim.file.blocks = sim.file.blocks + blkpad
      sim.mapTasks = sim.mapTasks + tskpad

  def reorderReduceTasks(self,sim,ignoreFileBitmap = False):
    tuples = []

    for i in xrange(0,len(sim.reduceTasks)):
      code = self.bc.getReduceTaskBitmap(sim,sim.reduceTasks[i])
      tuple = (code, \
        sim.reduceTasks[i])
      tuples.append(tuple)

    tuples = sorted(tuples, key = lambda x:x[0:2])
    for i in xrange(0,len(sim.reduceTasks)):
      sim.reduceTasks[i] = tuples[i][1]
