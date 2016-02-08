

class Conf(object):
  def __init__(self):
    self.NUMNODE = 6
    self.NUMRACK = 3
    """ TODO: change to NUMMAP """
    self.NUMMAP = 2
    self.NUMREDUCE = 2
    self.NUMBLOCK = self.NUMMAP
    """ TODO: change to MAPSTAGE """
    self.MAPSTAGE = 3
    self.SHUFFLESTAGE = 2
    self.NUMREPL = 3

    self.EnableStateCollapsing = True
    self.EnableTaskSymmetry = self.EnableStateCollapsing and True
    self.EnableDeepOpt = self.EnableStateCollapsing and True
    self.EnableOffRackReplica = False
    self.EnableTaskDelay = True
    self.PrintPermutations = True
    self.PrintGroupSummary = True

  def getRackID(self,node):
    return node % self.NUMRACK
