

from enum import Enum
import time, logging
from .mapred import *


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
  """ BIT ORDER (most to least significant):   """
  """   blockbits, maptaskbits, reducetaskbits """

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


  def getMapAttemptBitmap(self,sim,att):
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

  def getMapTaskBitmap(self,sim,task):
    bitset = self.conf.MAPSTAGE
    taskcode = reduce(lambda x,y: x*16 + \
      self.getMapAttemptBitmap(sim,y),task.attempts,0)
    return taskcode * 16**(bitset-len(task.attempts))

  def getMapTasksBitmap(self,sim):
    bitset = self.conf.MAPSTAGE
    return reduce(lambda x,y: x*(16**bitset) + \
      self.getMapTaskBitmap(sim,y)*(16**(bitset-len(y.attempts))),sim.getMapTasks(),0)


  def getReduceAttemptBitmap(self,sim,att):
    state = 0
    brn = (att.reducenode == sim.badnode) and (sim.badnode <> -1)
    brnr = (self.conf.getRackID(att.reducenode) == sim.badrack) \
      and (sim.badrack <> -1)
    state = state * 2 + brn
    state = state * 2 + brnr
    return state

  def getReduceTaskBitmap(self,sim,task):
    bitset = self.conf.SHUFFLESTAGE
    taskcode = reduce(lambda x,y: x*4 + \
      self.getReduceAttemptBitmap(sim,y),task.attempts,0)
    return taskcode * 4**(bitset-len(task.attempts))

  def getReduceTasksBitmap(self,sim):
    bitset = self.conf.SHUFFLESTAGE
    return reduce(lambda x,y: x*(4**bitset) + \
      self.getReduceTaskBitmap(sim,y)*(4**(bitset-len(y.attempts))),sim.getReduceTasks(),0)


  def getAllTasksBitmap(self,sim):
    mapbitset = self.conf.MAPSTAGE
    redbitset = self.conf.SHUFFLESTAGE
    bits = 0
    bits = bits * (16**(len(sim.getMapTasks())*mapbitset)) + self.getMapTasksBitmap(sim)
    bits = bits * (4**(len(sim.getReduceTasks())*redbitset)) + self.getReduceTasksBitmap(sim)
    return bits

  def getSimBitmap(self,sim):
    mapbitset = self.conf.MAPSTAGE
    redbitset = self.conf.SHUFFLESTAGE
    bits = self.getFileBitmap(sim)
    bits = bits * (16**(len(sim.getMapTasks())*mapbitset)) + self.getMapTasksBitmap(sim)
    bits = bits * (4**(len(sim.getReduceTasks())*redbitset)) + self.getReduceTasksBitmap(sim)
    return bits

  def getSimBitmapPartial(self,sim,size):
    blk = len(sim.getMapTasks())
    tskpad = []
    if size < blk:
      tskpad = sim.mapTasks[size:blk]
      sim.mapTasks = sim.mapTasks[0:size]

    code = self.getSimBitmap(sim)

    if tskpad:
      tasks = sim.getMapTasks()
      sim.mapTasks = sim.mapTasks + tskpad

    return code

  def getFormattedSimBitmap(self,sim):
    mapsBitLength = self.conf.MAPSTAGE*4
    redsBitLength = self.conf.SHUFFLESTAGE*2
    NUMBLOCK = self.conf.NUMBLOCK
    NUMREPL = self.conf.NUMREPL

    dnbit = ("{0:0" + str(2*NUMBLOCK*NUMREPL) + "b}").format(self.getFileBitmap(sim))
    dnstr = ",".join([dnbit[i:i+2*NUMREPL] for i in xrange(0,len(dnbit),2*NUMREPL)])

    mapBits = []
    for task in sim.getMapTasks():
      st = ("{0:0" + str(mapsBitLength) + "b}").format(self.getMapTaskBitmap(sim,task))
      taskbit = ",".join([st[i:i+4] for i in xrange(0,len(st),4)])
      mapBits.append(taskbit)
    taskstr = "-".join(mapBits)

    redBits = []
    for task in sim.getReduceTasks():
      st = ("{0:0" + str(redsBitLength) + "b}").format(self.getReduceTaskBitmap(sim,task))
      taskbit = ",".join([st[i:i+2] for i in xrange(0,len(st),2)])
      redBits.append(taskbit)
    redstr = "-".join(redBits)

    return dnstr+"|"+taskstr+"|"+redstr


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
    if sim.mapstage >= 0:
      for i in xrange(0,self.conf.NUMMAP):
        if sim.isMapSlow(sim.getMapTasks()[i].attempts[-1]):
          limp.append(i)
    return limp

  def isBadNode(self,topo,node):
    badnode = (node == topo.badnode) and (topo.badnode <> -1)
    badrack = (self.conf.getRackID(node) == topo.badrack) \
      and (topo.badrack <> -1)
    return badnode or badrack

  def hasBadDatasource(self,topo,att):
    if isinstance(att, MapAttempt):
      return self.isBadNode(topo,att.datanode)
    else:
      for map in top.getMapTasks():
        att = map.attempts[-1]
        if self.isBadNode(topo,att.mapnode):
          return True
      return False

  def hasBadCompute(self,topo,att):
    worker = -1
    if isinstance(att, MapAttempt):
      worker = att.mapnode
    else:
      worker = att.reducenode
    return self.isBadNode(topo,worker)

  def checkPermType(self,topo):
    """ This check assume there are only 2 task """
    tids = self.getLimpTaskIDs(topo)
    mapTasks = topo.getMapTasks()
    if len(mapTasks)<>2:
      #raise Exception("Topology has less/more than 2 tasks")
      return PermType.NORMAL if not tids else PermType.UNK

    if not tids:
      return PermType.NORMAL
    elif len(tids) == len(mapTasks):
      # FS type
      verd = [self.hasBadDatasource(topo,mapTasks[x].attempts[-1]) for x in tids]
      verd = sorted(verd)
#      print tids, verd
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
      verd = mapTasks[tids[0]].attempts
      verd = [self.hasBadDatasource(topo,x) for x in verd[len(verd)-2:]]
#      print tids, verd
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

class PermStringType(PermTypeChecker):
  def __init__(self,conf):
    super(PermStringType, self).__init__(conf)

  def checkPermType(self,topo):
    str = ""
    mapscore = 1.0
    shufflescore = 1.0/3.0

    if topo.getMapProg()>=mapscore:
      str += "(NM)"
    else:
      mapStat = []
      mapTasks = topo.getMapTasks()
      for i in range(0,len(mapTasks)):
        if topo.getMapTaskProg(i)>=mapscore:
          mapStat.append("A")
        else:
          attStat = []
          for att in mapTasks[i].attempts:
            if self.hasBadDatasource(topo,att):
              attStat.append("D")
            else:
              attStat.append("C")
          mapStat.append("".join(attStat))
      str += ",".join(sorted(mapStat))
      return str

    str += "|"
    if topo.getShuffleProg()>=shufflescore:
      str += "(NR)"
    else:
      redStat = []
      redTasks = topo.getReduceTasks()
      for i in range(0,len(redTasks)):
        if topo.getReduceTaskProg(i)>=shufflescore:
          redStat.append("A")
        else:
          attStat = []
          for att in redTasks[i].attempts:
            if self.hasBadCompute(topo,att):
              attStat.append("C")
            else:
              attStat.append("D")
          redStat.append("".join(attStat))
      str += ",".join(sorted(redStat))

    return str


class Printer(object):
  def __init__(self, conf):
    self.bc = Bitcoder(conf)
#    self.ck = PermTypeChecker(conf)
    self.ck = PermStringType(conf)
    self.conf = conf

  def getTaskTopology(self,sim):
    topo = []
    for i in xrange(0,len(sim.getMapTasks())):
      task = sim.getMapTasks()[i]
      for j in xrange(0,len(task.attempts)):
        att = task.attempts[j]
        tuple = ("m%d_%d" % (i,j), att.datanode, att.mapnode)
        if (tuple[1] == -1) and (tuple[2] == -1):
          continue
        else:
          topo.append(tuple)
    for i in xrange(0,len(sim.getReduceTasks())):
      task = sim.getReduceTasks()[i]
      for j in xrange(0,len(task.attempts)):
        att = task.attempts[j]
        tuple = ("r%d_%d" % (i,j), att.reducenode)
        if (tuple[1] == -1) and (tuple[2] == -1):
          continue
        else:
          topo.append(tuple)
    return topo

  def isLimplock(self,sim):
    limp = False
    if sim.mapstage >= 0:
      for i in xrange(0,self.conf.NUMMAP):
        limp = limp or sim.isMapSlow(sim.getMapTasks()[i].attempts[-1])
    if sim.reducestage >= 0:
      for i in xrange(0,self.conf.NUMREDUCE):
        limp = limp or sim.isReduceSlow(sim.getReduceTasks()[i].attempts[-1])
    return limp

  def printSim(self,sim):
    print (sim.badnode,sim.badrack)
    print sim.file.blocks
    for t in sim.getMapTasks():
      print [(x.datanode,x.mapnode) for x in t.attempts]

  def printPerm(self,k,v):
    print "Hash key: ", k
    if self.conf.EnableStateCollapsing:
      print "Hash bit: ", self.bc.getFormattedSimBitmap(v)
    print "Job prog: ", v.getMapProg()
    print "Map stage: ", v.mapstage
    print "Red stage: ", v.reducestage
    print "Total count: ", v.getCount()
    print "Probability: ", v.prob
    print "Bad node: ", v.badnode
    print "Bad rack: ", v.badrack
    print "Topology: ", self.getTaskTopology(v)
    print "Datanodes: ", v.file.blocks
    print "IsLimplock:", self.isLimplock(v)
    print "PermType:", self.ck.checkPermType(v)
    print "====================================="

  def printPermGroups(self,queue):
    tuples = map(lambda x: ((self.bc.getFileBitmap(x),self.bc.getAllTasksBitmap(x)),x), queue)
    groups = dict()
    for k,v in sorted(tuples, key=lambda x:x[0]):
      key = self.ck.checkPermType(v)
      if key in groups:
        (key,ct,ptg) = groups[key]
        ct  += v.getCount()
        ptg += v.prob
        groups[key] = (key,ct,ptg)
      else:
        (key,ct,ptg) = (key,v.getCount(),v.prob)
        groups[key] = (key,ct,ptg)

    print "Perm Type\tTotal count\tProbability"
    for k,v in sorted(groups.items()):
      print "%s\t%d\t%f" % (k,v[1],v[2])
    print ""


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
    print "=====================================\n"

    if self.conf.PrintGroupSummary:
      self.printPermGroups(queue)

    if self.conf.PrintPermutations:
      tuples = map(lambda x: ((self.bc.getFileBitmap(x),self.bc.getAllTasksBitmap(x)),x), queue)
      for k,v in sorted(tuples, key=lambda x:x[0]):
        self.printPerm(k,v)

