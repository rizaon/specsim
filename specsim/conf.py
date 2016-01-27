

class Conf(object):
  def __init__(self):
    self.NUMNODE = 6
    self.NUMRACK = 3
    self.NUMTASK = 5
    self.NUMBLOCK = self.NUMTASK
    self.NUMSTAGE = 3
    self.NUMREPL = 3

    self.EnableStateCollapsing = True
    self.EnableTaskSymmetry = self.EnableStateCollapsing and True
    self.EnableDeepOpt = self.EnableStateCollapsing and True
    self.EnableOffRackReplica = False
    self.PrintPermutations = True

  def getRackID(self,node):
    return node % self.NUMRACK
