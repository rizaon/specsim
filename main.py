#!/usr/bin/python


from specsim.conf import Conf
from specsim.mapred import *
from specsim.util import *
from specsim.algo import *
import logging


logging.basicConfig(filename="specsim.log",\
  filemode="w",level=logging.INFO)

CONF = Conf()

#SPEC = BasicSE(CONF)
#SPEC = FAReadSE(CONF)
SPEC = PathSE(CONF)
BC = Bitcoder(CONF)
OPT = Optimizer(CONF)
PRINT = Printer(CONF)
TIME = TimeReporter()


def permuteFailure():
  TIME.start()

  failurequeue = []
  totalfailure = CONF.NUMNODE+CONF.NUMRACK

  sim = SimTopology(CONF,(0,-1))
  sim.prob = 0.5
  failurequeue.append(sim)
  sim = SimTopology(CONF,(-1,0))
  sim.prob = 0.5
  failurequeue.append(sim)

  """for i in xrange(0,CONF.NUMNODE):
    sim = SimTopology(CONF,(i,-1))
    sim.prob /= totalfailure
    failurequeue.append(sim)
  for i in xrange(0,CONF.NUMRACK):
    sim = SimTopology(CONF,(-1,i))
    sim.prob /= totalfailure
    failurequeue.append(sim)"""

  TIME.stop()
  TIME.report("Failure permutation complete!",failurequeue)
  return failurequeue

def placeBlock(queue, blockid):
  TIME.start()

  blocks = [[x] for x in xrange(0,CONF.NUMNODE)]
  for rep in xrange(1,CONF.NUMREPL):
    tmp = blocks
    blocks = []
    for blk in tmp:
      for i in xrange(0,CONF.NUMNODE):
        if i not in blk:
          blocks.append(blk+[i])

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    for blk in blocks:
      psim = sim.clone()
      psim.file.blocks[blockid] = blk
      psim.prob /= len(blocks)
      queue.append(psim)


  TIME.stop()
  TIME.report("Block %d permutation done!" % blockid,queue)
  return queue

def permuteBlock(queue):
  for i in xrange(0,CONF.NUMBLOCK):
    queue = placeBlock(queue,i)
    if CONF.EnableDeepOpt:
      queue = reduceBlockPerms(queue, i+1)
  return queue

def reduceBlockPerms(queue,blocksize):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderBlocksPartial(sim,blocksize)
    id = BC.getSimBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      sameperm.count += sim.count
      sameperm.prob += sim.prob
    else:
      ret[id] = sim
  ret = ret.values()

  TIME.stop()
  TIME.report("Reduction of block permutation done!" ,ret)
  return ret


def placeOriginalTask(queue,taskid):
  TIME.start()

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    attempts = []
    for i in sim.file.blocks[taskid]:
      for j in xrange(0,CONF.NUMNODE):
        attempts.append(MapAttempt(i,j))
    for att in attempts:
      psim = sim.clone()
      psim.addAttempt(taskid,att,True)
      psim.prob /= len(attempts)
      queue.append(psim)

  TIME.stop()
  TIME.report("Task %d permutation done!" % taskid,queue)
  return queue

def permuteOriginalTask(queue):
  for sim in queue:
    sim.moveMapStageUp()

  for i in xrange(0,CONF.NUMMAP):
    queue = placeOriginalTask(queue,i)
    if CONF.EnableDeepOpt:
      queue = reduceTaskPerms(queue, i+1)

  for sim in queue:
    sim.updateProgress()
  return queue


def placeOriginalReduceTask(queue,taskid):
  TIME.start()

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    attempts = []
    for j in xrange(0,CONF.NUMNODE):
      attempts.append(ReduceAttempt(j))
    for att in attempts:
      psim = sim.clone()
      psim.addAttempt(taskid,att,False)
      psim.prob /= len(attempts)
      queue.append(psim)

  TIME.stop()
  TIME.report("Reduce task %d permutation done!" % taskid,queue)
  return queue

def permuteOriginalReduceTask(queue):
  for sim in queue:
    sim.moveReduceStageUp()

  for i in xrange(0,CONF.NUMREDUCE):
    queue = placeOriginalReduceTask(queue,i)
    if CONF.EnableDeepOpt:
      queue = reduceTaskPerms(queue, CONF.NUMMAP)

  for sim in queue:
    sim.updateProgress()
  return queue


def reduceTaskPerms(queue,tasksize):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderTasksPartial(sim,tasksize)
      OPT.reorderReduceTasks(sim)
    id = BC.getSimBitmapPartial(sim,tasksize)
    if id in ret:
      sameperm = ret[id]
#      if (BC.getSimBitmap(sim)!=BC.getSimBitmap(sameperm)):
#        print "VIOLATION: %d" % id
#        PRINT.printPerm(BC.getSimBitmap(sameperm),sameperm)
#        PRINT.printPerm(BC.getSimBitmap(sim),sim)
      sameperm.count += sim.count
      sameperm.prob += sim.prob
    else:
      ret[id] = sim
  ret = ret.values()

  TIME.stop()
  TIME.report("Reduction of task permutation done!" ,ret)
  return ret


def placeBackupTask(queue,taskid):
  TIME.start()
  nobackup = []

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    if not SPEC.needBackup(sim,taskid):
      nobackup.append(sim)
    else:
      attempts = SPEC.getPossibleBackups(sim,taskid)
      for att in attempts:
        psim = sim.clone()
        psim.addAttempt(taskid,att,True)
        psim.prob /= len(attempts)
        queue.append(psim)

  queue = nobackup + queue
  TIME.stop()
  TIME.report("Backup task %d permutation done!" % taskid,queue)
  return queue

def permuteBackupTask(queue):
  for sim in queue:
    sim.moveMapStageUp()

  if isinstance(SPEC, PathSE):
#    (speced,queue) = SPEC.specPathGroup(queue)
    for i in xrange(0,CONF.NUMMAP):
      queue = placeBackupTask(queue,i)
#    queue = speced + queue
  else:
    for i in xrange(0,CONF.NUMMAP):
      queue = placeBackupTask(queue,i)

  for sim in queue:
    sim.updateProgress()

  return queue


def placeReduceBackupTask(queue,taskid):
  TIME.start()
  nobackup = []

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    if not SPEC.needReduceBackup(sim,taskid):
      nobackup.append(sim)
    else:
      attempts = SPEC.getPossibleReduceBackups(sim,taskid)
      for att in attempts:
        psim = sim.clone()
        psim.addAttempt(taskid,att,True)
        psim.prob /= len(attempts)
        queue.append(psim)

  queue = nobackup + queue
  TIME.stop()
  TIME.report("Backup task %d permutation done!" % taskid,queue)
  return queue

def permuteReduceBackupTask(queue):
  for sim in queue:
    sim.moveReduceStageUp()

  if isinstance(SPEC, PathSE):
#    (speced,queue) = SPEC.specPathGroup(queue)
    for i in xrange(0,CONF.NUMREDUCE):
      queue = placeReduceBackupTask(queue,i)
#    queue = speced + queue
  else:
    for i in xrange(0,CONF.NUMREDUCE):
      queue = placeReduceBackupTask(queue,i)

  for sim in queue:
    sim.updateProgress()

  return queue


def reduceByTasksBitmap(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderTasks(sim, CONF.NUMMAP)
    id = BC.getAllTasksBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      if BC.getFileBitmap(sameperm) > BC.getFileBitmap(sim):
        sim.count += sameperm.count
        sim.prob += sameperm.prob
        ret[id] = sim
      else:
        sameperm.count += sim.count
        sameperm.prob += sim.prob
    else:
      ret[id] = sim
  ret = ret.values()

  TIME.stop()
  TIME.report("Reduction by task bitmap done!" ,ret)
  return ret


def main():
  timer = TimeReporter()
  timer.start()
  simqueue = permuteFailure()

  """ stage -1: permute datablocks  """
  if CONF.MAPSTAGE > -1:
    simqueue = permuteBlock(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceBlockPerms(simqueue, CONF.NUMBLOCK)
    timer.stop()
    timer.report("Up to block placement",simqueue)

    if CONF.MAPSTAGE == 0:
      PRINT.printPerms(simqueue)
      exit(0)    

  """ stage  0: permute original tasks """
  if CONF.MAPSTAGE > 0:
    simqueue = permuteOriginalTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue, CONF.NUMMAP)
    timer.stop()
    timer.report("Up to task placement",simqueue)

  """ stage  0: permute original reduce tasks """
  if CONF.SHUFFLESTAGE > 0:
    simqueue = permuteOriginalReduceTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue, CONF.NUMMAP)
    timer.stop()
    timer.report("Up to reduce task placement",simqueue)


  if CONF.MAPSTAGE == 1:
    PRINT.printPerms(simqueue)
    exit(0)

  """ Task delaying """
  if CONF.EnableTaskDelay and isinstance(SPEC, PathSE):
    simqueue = SPEC.filterForDelay(simqueue)
#   for sim in SPEC.delayed:
#     PRINT.printPerm(0,sim)


  """ stage  1 - (MAPSTAGE-1): run SE """
  numattempt = 1
  while numattempt < CONF.MAPSTAGE:
    numattempt += 1
    simqueue = permuteBackupTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue, CONF.NUMMAP)
    timer.stop()
    timer.report("Up to %dth map attempt placement" % numattempt,simqueue)


  if CONF.EnableStateCollapsing:
    simqueue = reduceByTasksBitmap(simqueue)
  if isinstance(SPEC, PathSE):
    SPEC.printPathSEStat()
  PRINT.printPerms(simqueue)

  """print "***********DEBUG***********"
  for sim in simqueue:
    if PRINT.isLimplock(sim):
      PRINT.printPerm(0,sim)
      for task in sim.tasks:
        SPEC.debug_PrintPoint(sim,task,len(task.attempts)-1)"""

def test_ShouldSpec():
  sim = SimTopology(CONF,(0,-1))
  sim.runstage = 0
  sim.file.blocks = [[1, 0, 3], [1, 0, 3]]
  sim.addAttempt(0,MapAttempt(0,0),True)
  sim.addAttempt(1,MapAttempt(0,0),True)
  sim.moveMapStageUp()
  sim.updateProgress()
  print sim.isSlow(sim.tasks[0].attempts[0])
  print SPEC.hasBasicSpec(sim)
  PRINT.printPerm((BC.getMapTasksBitmap(sim),BC.getFileBitmap(sim)),sim)

if __name__ == '__main__':
  main()
#  test_ShouldSpec()

