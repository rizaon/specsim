

class Conf(object):
  def __init__(self):
    self.NUMNODE = 6
    self.NUMRACK = 3
    self.NUMTASK = 2
    self.NUMBLOCK = self.NUMTASK
    self.NUMSTAGE = 2
    self.NUMREPL = 2

    self.EnableStateCollapsing = True
    self.EnableTaskSymmetry = self.EnableStateCollapsing and True
    self.EnableOffRackReplica = False
    self.PrintPermutations = True

  def getRackID(self,node):
    return node % self.NUMRACK

