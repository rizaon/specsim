

import copy

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
    self.prob = 1.0

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
    klon.prob = self.prob

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

