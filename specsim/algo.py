

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

