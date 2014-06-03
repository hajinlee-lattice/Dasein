'''
Created on May 6, 2014

@author: hliu
'''

from ledp_pylib import load, create_samples
from ledp_parallel import parallel_run
from ledp_variables import *
import argparse, time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='num_samples', default='3')
    args = parser.parse_args()
    
    argslist = [jetty_host, db_creds, table, key_cols, metadata_table]
    num_apps = len(customers)
    MAX_THREADS = num_apps + 1
    
    parallel_run(load, argslist, MAX_THREADS, False)       
    print "All load requests submitted!"
    
    time.sleep(300)
    argslist = [jetty_host, table, training_percentage, args.num_samples]
    parallel_run(create_samples, argslist, MAX_THREADS, True)   
    print "All load requests submitted!"
    

    
if __name__ == '__main__':
    main()
