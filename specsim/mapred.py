

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
  def __init__(self, reducenode):
    self.reducenode = reducenode

  def clone(self):
    return ReduceAttempt(self.reducenode)


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
    for i in xrange(0,self.conf.NUMMAP):
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

    if (stage >= 0):
      # calc map progress
      self.mapProgress = \
        reduce(lambda x,y: x+self.getMapTaskProg(y), \
        range(0,len(self.mapTasks)), .0) / len(self.mapTasks)

    if (stage >= self.conf.MAPSTAGE):
      # calc shuffle progress
      self.shuffleProgress = \
        reduce(lambda x,y: x+self.getReduceTaskProg(y), \
        range(0,len(self.reduceTasks)), .0) / len(self.reduceTasks)

  def isBadPath(self,source,sink):
    badSource = (source == self.badnode) and (self.badnode <> -1)
    badSink   = (sink == self.badnode) and (self.badnode <> -1)
    badSourceRack = (self.conf.getRackID(source) == self.badrack) \
      and (self.badrack <> -1)
    badSinkRack   = (self.conf.getRackID(sink) == self.badrack) \
      and (self.badrack <> -1)
    return (badSource ^ badSink) or (badSourceRack ^ badSinkRack)

  def isMapSlow(self,att):
    assert isinstance(att, MapAttempt)
    return self.isBadPath(att.datanode, att.mapnode)

  def isReduceSlow(self,att):
    assert isinstance(att, ReduceAttempt)
    for i in xrange(0,len(self.mapTasks)):
      if self.getMapTaskProg(i)>=1.0:
        mapnode = self.mapTasks[i].attempts[-1].mapnode
        if self.isBadPath(mapnode,att.reducenode):
          return True
      else:
        return True
    return False

  def getMapProg(self):
    return self.mapProgress

  def getShuffleProg(self):
    return self.shuffleProgress

  """TODO: fix me to max(attempt progress)"""
  def getMapTaskProg(self,tid):
    att = self.mapTasks[tid].attempts[-1]
    return 0.0 if self.isMapSlow(att) else 1.0

  """TODO: fix me to max(attempt progress)"""
  def getReduceTaskProg(self,tid):
    att = self.reduceTasks[tid].attempts[-1]
    tp = (1.0/3.0)/len(att.mapTasks)
    prog = 0.0
    for i in xrange(0,len(self.mapTasks)):
      if self.getMapTaskProg(i)>=1.0:
        mapnode = self.mapTasks[i].attempts[-1].mapnode
        if not self.isBadPath(mapnode,att.reducenode):
          prog += tp
    return prog

  def getCount(self):
    return self.count

