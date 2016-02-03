

import copy

class Attempt(object):
  def __init__(self, datanode, mapnode):
    self.datanode = datanode
    self.mapnode = mapnode

  def clone(self):
    return Attempt(self.datanode,self.mapnode)

class MapAttempt(Attempt):
  def __init__(self, datanode, mapnode):
    super(MapAttempt, self).__init__(datanode,mapnode)

  def clone(self):
    return MapAttempt(self.datanode,self.mapnode)

class ReduceAttempt(Attempt):
  def __init__(self, mapnodes, reducenode):
    self.mapnodes = mapnodes
    self.reducenode = reducenode

  def clone(self):
    return ReduceAttempt(self.mapnodes,self.reducenode)


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

class MapTask(Task):
  def __init__(self):
    super(MapTask, self).__init__()

  def clone(self):
    ctask = MapTask()
    for att in self.attempts:
      ctask.addAttempt(att.clone())
    return ctask

class ReduceTask(Task):
  def __init__(self):
    super(ReduceTask, self).__init__()

  def clone(self):
    ctask = ReduceTask()
    for att in self.attempts:
      ctask.addAttempt(att.clone())
    return ctask


class SimTopology(object):
  def __init__(self, conf, failure):
    self.conf = conf
    self.runstage = -1
    self.mapProgress = .0
    self.shuffleProgress = .0
    self.currentstate = 0
    self.count = 1
    self.prob = 1.0

    self.file = HdfsFile(self.conf.NUMBLOCK, self.conf.NUMREPL)

    self.mapTasks = []
    self.reduceTasks = []
    for i in xrange(0,self.conf.NUMTASK):
      self.mapTasks.append(MapTask())

    self.badnode = failure[0]
    self.badrack = failure[1]

  def addAttempt(self,tid,att,isMap):
    tasks = self.getMapTasks() if isMap else self.getReduceTasks()
    task = tasks[tid]
    task.addAttempt(att)

  def clone(self):
    klon = SimTopology(self.conf,(self.badnode,self.badrack))
    klon.runstage = self.runstage
    klon.mapProgress = self.mapProgress
    klon.shuffleProgress = self.shuffleProgress
    klon.currentstate = self.currentstate
    klon.count = self.count
    klon.prob = self.prob

    klon.file = self.file.clone()
    klon.mapTasks = [task.clone() for task in self.mapTasks]
    klon.reduceTasks = [task.clone() for task in self.reduceTasks]

    return klon

  def getMapTasks(self):
    return self.mapTasks

  def getReduceTasks(self):
    return self.reduceTasks

  def setBlocks(self,blockid,repl):
    self.file.blocks[blockid] = repl

  def moveStageUp(self):
    self.runstage += 1

  def updateProgress(self):
    stage = self.runstage + 1

    if (self.runstage >= 0):
      # calc map progress
      tp = 1.0/(len(self.getMapTasks()))
      self.mapProgress = \
        reduce(lambda x,y: x+self.getMapTaskProg(y), \
        range(0,len(self.mapTasks)), .0) / len(self.mapTasks)

  def isMapSlow(self,att):
    assert isinstance(att, MapAttempt)

    bdn = (att.datanode == self.badnode) and (self.badnode <> -1)
    bmp = (att.mapnode == self.badnode) and (self.badnode <> -1)
    bdnr = (self.conf.getRackID(att.datanode) == self.badrack) \
      and (self.badrack <> -1)
    bmpr = (self.conf.getRackID(att.mapnode) == self.badrack) \
      and (self.badrack <> -1)
    return ((not bdn and bmp) or (bdn and not bmp)) or \
      ((not bdnr and bmpr) or (bdnr and not bmpr))

  def getMapProg(self):
    return self.mapProgress

  def getShuffleProg(self):
    return self.shuffleProgress


  """TODO: fix me to max(attempt progress)"""
  def getMapTaskProg(self,tid):
    att = self.mapTasks[tid].attempts[-1]
    return 0.0 if self.isMapSlow(att) else 1.0

  def getCount(self):
    return self.count

