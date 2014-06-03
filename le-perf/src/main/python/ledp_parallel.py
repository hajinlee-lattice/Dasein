'''
Created on May 13, 2014

@author: hliu
'''
from ledp_variables import *
from threading import Thread
import time, threading

def parallel_run(job_type, argslist, max_threads, join):
    threadlist = []
    print "Num of threads running now is " + str(threading.activeCount())
    for customer in customers:
        argslist.append(customer)
        while True:
            if threading.activeCount() < max_threads:
                t = Thread(target=job_type, args=tuple(argslist))
                t.setDaemon(True)
                t.start()
                threadlist.append(t)
                break
            else:
                time.sleep(0.3)
        argslist.remove(customer)
     #   time.sleep(0.6)
    print "Num of threads running now is " + str(threading.activeCount())
    if join:
        for t in threadlist:
            t.join()
