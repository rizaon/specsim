#!/usr/bin/python
import sys
import matplotlib.pyplot as plt

def main():
  numtask = int(sys.argv[1])
  slow = [x/10.0 for x in range(11,50)]
  x = []
  y = []
  gx = []
  gy = []
  for i in xrange(0,numtask+1):
    for slowdown in slow:
      good = numtask - i
      bad = i
      badprog = 100.0 / slowdown
      avgprog = (100.0 * good + badprog * bad) / numtask
      badperc = 100.0 * bad / numtask
      if (bad>0) and (badprog < 100.0) and (avgprog - badprog < 20.0):
        x.append(badperc)
        y.append(slowdown)
      else:
        gx.append(badperc)
        gy.append(slowdown)

#  plt.scatter(x, y)
#  plt.scatter(gx,gy)
#  plt.grid(True)
#  plt.show()

  f = open("nospec.txt", "w")
  for i in xrange(0,len(x)):
    f.write("%f %f\n" % (x[i],y[i]))
  f.close()

  f = open("spec.txt", "w")
  for i in xrange(0,len(gx)):
    f.write("%f %f\n" % (gx[i],gy[i]))
  f.close()

if __name__ == '__main__':
  main()
