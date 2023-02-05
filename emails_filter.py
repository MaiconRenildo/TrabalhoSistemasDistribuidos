import sys, os, time, math, re
from pyDF import *

sys.path.append(os.environ['PYDFHOME'])
	
def filter_emails(args):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
	
    email = args[0]
    if(re.match(regex, email)):
        return email
    return ""

def print_emails(args):
    if args[0] != "":
        print args[0]

nprocs = int(sys.argv[1])
filename = sys.argv[2]

graph = DFGraph()
sched = Scheduler(graph, nprocs, mpi_enabled = False)

fp = open(filename, "r")

src = Source(fp)
graph.add(src)

nd = FilterTagged(filter_emails, 1)
graph.add(nd)

ser = Serializer(print_emails, 1)
graph.add(ser)

src.add_edge(nd, 0)
nd.add_edge(ser, 0)

t0 = time.time()
sched.start()
t1 = time.time()

print "Execution time %.3f" %(t1-t0)
