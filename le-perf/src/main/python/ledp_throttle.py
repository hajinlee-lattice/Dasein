'''
Created on May 6, 2014

@author: hliu
'''

from algorithm import Algorithm
from ledp_pylib import submit_model
from ledp_variables import *
from ledp_parallel import parallel_run
import time

def main():
    lr = Algorithm("lr", 1 , 64, 0)
    dt = Algorithm("dt", 1, 64, 0)
    rf = Algorithm("rf", 1, 64, 0)
    algorithms = [lr, dt, rf]
    
    argslist = [jetty_host, "ModelSubmission", table, features, target, key_cols, algorithms]
    parallel_run(submit_model, argslist)

if __name__ == '__main__':
    main()
