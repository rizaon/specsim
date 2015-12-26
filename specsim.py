#!/usr/bin/python

NUMNODE = 6
NUMRACK = 3
NUMTASK = 2

FAILED = 0
SUCCESS = 0

def getRackID(node):
  return node % NUMRACK

class SimContext(object):
  """ fail is tuple of (nodeid,rackid) """
  def __init__(self, fail):
    self.attempts = []
    for i in xrange(0,NUMTASK):
      self.attempts.append([])
    self.failure = fail

  def isMapFail(self,(dn,map)):
    (bnode,brack) = self.failure
    dnf = (getRackID(dn) == brack)
    mpf = (getRackID(map) == brack)
    
    """" fail if one node is bad node, or ONLY one belongs to bad rack """
    return (dn==bnode) or (map==bnode) or \
      ((dnf or mpf) and (not dnf or not mpf))
    

def permuteTasks(simcon, tid):
  if tid >= 0:
    if len(simcon.attempts[tid]) > 2:
      # task has 2 attempts already
      global FAILED
      FAILED += 1
    else:
      for i in xrange(0,NUMNODE):
        for j in xrange(0,NUMNODE):
          if (len(simcon.attempts[tid]) > 0) and \
             (simcon.attempts[tid][0][1] == j):
            # SE kick in here, do not assign to same node
            continue
          simcon.attempts[tid].append((i,j))
          if simcon.isMapFail((i,j)): 
            # this task is straggling, speculate!
            permuteTasks(simcon,tid)
          else:
            # this task run well, permute next task
            permuteTasks(simcon,tid-1)
          simcon.attempts[tid].pop()
  else:
    # all tasks run normaly
    global SUCCESS
    SUCCESS += 1

def main():
  """ node failure """
  for i in xrange(0,1):
    # only permute node 0 as bad node for now
    simcon = SimContext((i,-1))
    permuteTasks(simcon,NUMTASK-1)

  """ rack failure """
  for i in xrange(0,1):
    # only permute rack 0 as bad rack for now
    simcon = SimContext((-1,i))
    permuteTasks(simcon,NUMTASK-1)

  print "TOTL: ", SUCCESS+FAILED
  print "SUCC: ", SUCCESS
  print "FAIL: ", FAILED

if __name__ == '__main__':
  main()

