

from abc import ABCMeta, abstractmethod
from .util import Bitcoder
from .mapred import *

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


class FAReadSE(Speculator):
  def __init__(self,conf):
    self.conf = conf

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
    self.HARDPREF = False

  def toPointDict(self,lst):
    d = {}
    for a in lst:
      if a not in d:
        d[a] = 1
      else:
        d[a] += 1
    return d

  def getNegPref(self,att,triedNM,triedMR,triedDR):
    mRack = self.conf.getRackID(att.mapnode)
    dRack = self.conf.getRackID(att.datanode)

    retryDN = triedNM.get(att.datanode,0)
    retryMN = triedNM.get(att.mapnode,0)
    sameMR  = triedMR.get(mRack,0)
    sameDR  = triedDR.get(dRack,0)

    pref = 0
    pref = pref*10 + retryMN
    pref = pref*10 + retryDN
    pref = pref*10 + sameMR
    pref = pref*10 + sameDR
    return pref

  def getPossibleBackups(self,sim,tid):
    atts = sim.tasks[tid].attempts
    triedMN = [a.mapnode for a in atts]
    triedDN = self.toPointDict([a.datanode for a in atts] + triedMN)
    triedMR = self.toPointDict([self.conf.getRackID(a.mapnode) for a in atts])
    triedDR = self.toPointDict([self.conf.getRackID(a.datanode) for a in atts])

    backups = []
    locatedDN = sim.file.blocks[tid]
    for dn in locatedDN:
      for map in xrange(0,self.conf.NUMNODE):
        if map not in triedMN:
          att = Attempt(dn,map)
          pref =  self.getNegPref(att,triedDN,triedMR,triedDR)
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

