#!/usr/bin/python


from specsim.conf import Conf
from specsim.mapred import *
from specsim.util import *
from specsim.algo import *
import logging


logging.basicConfig(filename="specsim.log",\
  filemode="w",level=logging.INFO)

CONF = Conf()
CONF.NUMSTAGE = 3
CONF.PrintPermutations = True

#SPEC = BasicSE(CONF)
#SPEC = FAReadSE(CONF)
SPEC = PathSE(CONF)
SPEC.HARDPREF = True
BC = Bitcoder(CONF)
OPT = Optimizer(CONF)
PRINT = Printer(CONF)
TIME = TimeReporter()


def permuteFailure():
  TIME.start()

  failurequeue = []
  totalfailure = CONF.NUMNODE+CONF.NUMRACK
  for i in xrange(0,CONF.NUMNODE):
    sim = SimTopology(CONF,(i,-1))
    sim.prob /= totalfailure
    failurequeue.append(sim)
  for i in xrange(0,CONF.NUMRACK):
    sim = SimTopology(CONF,(-1,i))
    sim.prob /= totalfailure
    failurequeue.append(sim)

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
  return queue

def reduceBlockPerms(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderBlocks(sim)
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
        attempts.append(Attempt(i,j))
    for att in attempts:
      psim = sim.clone()
      psim.addAttempt(taskid,att)
      psim.prob /= len(attempts)
      queue.append(psim)

  TIME.stop()
  TIME.report("Task %d permutation done!" % taskid,queue)
  return queue

def permuteOriginalTask(queue):
  for i in xrange(0,CONF.NUMTASK):
    queue = placeOriginalTask(queue,i)
  for sim in queue:
    sim.updateProgress()
  return queue

def reduceTaskPerms(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderTasks(sim)
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


def placeBackupTask(queue,taskid):
  TIME.start()
  nobackup = []

  tmp = queue
  queue = []
  while len(tmp)>0:
    sim = tmp.pop(0)
    if not sim.needBackup(taskid):
      nobackup.append(sim)
    else:
      attempts = SPEC.getPossibleBackups(sim,taskid)
      for att in attempts:
        psim = sim.clone()
        psim.addAttempt(taskid,att)
        psim.prob /= len(attempts)
        queue.append(psim)

  queue = nobackup + queue
  TIME.stop()
  TIME.report("Backup task %d permutation done!" % taskid,queue)
  return queue

def permuteBackupTask(queue):
  for i in xrange(0,CONF.NUMTASK):
    queue = placeBackupTask(queue,i)
  for sim in queue:
    sim.updateProgress()
  return queue


def reduceByTasksBitmap(queue):
  TIME.start()

  ret = dict()
  for sim in queue:
    if CONF.EnableTaskSymmetry:
      OPT.reorderTasks(sim, True)
    id = BC.getTasksBitmap(sim)
    if id in ret:
      sameperm = ret[id]
      if BC.getFileBitmap(sameperm) < BC.getFileBitmap(sim):
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
  if CONF.NUMSTAGE > -1:
    simqueue = permuteBlock(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceBlockPerms(simqueue)
    timer.stop()
    timer.report("Up to block placement",simqueue)

    if CONF.NUMSTAGE == 0:
      PRINT.printPerms(simqueue)
      exit(0)    

  """ stage  0: permute original tasks """
  if CONF.NUMSTAGE > 0:
    simqueue = permuteOriginalTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue)
    timer.stop()
    timer.report("Up to task placement",simqueue)

    if CONF.NUMSTAGE == 1:
      PRINT.printPerms(simqueue)
      exit(0)

  """ stage  1: run SE """
  numbackup = 1
  while numbackup < CONF.NUMSTAGE:
    simqueue = permuteBackupTask(simqueue)
    if CONF.EnableStateCollapsing:
      simqueue = reduceTaskPerms(simqueue)
    timer.stop()
    timer.report("Up to %dth backup placement" % numbackup,simqueue)
    numbackup += 1


  if CONF.EnableStateCollapsing:
    simqueue = reduceByTasksBitmap(simqueue)
  PRINT.printPerms(simqueue)

if __name__ == '__main__':
  main()

