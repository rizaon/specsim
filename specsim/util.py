

from enum import Enum
import time, logging


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
    logging.info(msg+"\nTime elapsed: %f\nQueue size: %d\n",\
      self.getElapsed(), len(queue))


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
      verd = [self.hasBadDatasource(topo,topo.tasks[x].attempts[-1]) for x in tids]
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
      verd = topo.tasks[tids[0]].attempts
      verd = [self.hasBadDatasource(topo,x) for x in verd[len(verd)-2:]]
      print verd
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

  def printPerm(self,k,v):
    print "Hash key: ", k
    if self.conf.EnableStateCollapsing:
      print "Hash bit: ", self.bc.getFormattedSimBitmap(v)
    print "Total count: ", v.getCount()
    print "Probability: ", v.prob
    print "Bad node: ", v.badnode
    print "Bad rack: ", v.badrack
    print "Topology: ", self.getTaskTopology(v)
    print "Datanodes: ", v.file.blocks
    print "IsLimplock:", self.isLimplock(v)
    print "PermType:", self.ck.checkPermType(v)
    print "====================================="


  def printPerms(self,queue):
    uniquePerm = len(queue)
    uniqueSucc = 0
    uniqueFail = 0
    totalPerm = 0
    totalSucc = 0
    totalFail = 0
    succprob = .0
    failprob = .0

    for v in queue:
      limp = self.isLimplock(v)
      uniqueSucc += not limp
      uniqueFail += limp
      totalPerm += v.getCount()
      totalSucc += v.getCount() if not limp else 0
      totalFail += v.getCount() if limp else 0
      succprob += v.prob if not limp else 0
      failprob += v.prob if limp else 0

    print "Unique permutation: ", uniquePerm
    print "Unique success: ", uniqueSucc
    print "Unique failure: ", uniqueFail
    print "Total permutation: ", totalPerm
    print "Total success: ", totalSucc
    print "Total failure: ", totalFail
    print "Coverage: ", succprob+failprob
    print "Succ prob: ", succprob
    print "Fail prob: ", failprob
    print "====================================="
    if self.conf.PrintPermutations:
      tuples = map(lambda x: ((self.bc.getTasksBitmap(x),self.bc.getFileBitmap(x)),x), queue)
      for k,v in sorted(tuples, key=lambda x:x[0]):
        self.printPerm(k,v)
