'''
Created on May 6, 2014

@author: hliu
'''

from algorithm import Algorithm
from ledp_pylib import submit_model
from ledp_pylib import get_num_apps
from ledp_variables import *
from ledp_parallel import parallel_run
import time, os, csv


def main():   
    start_num_finished_apps = 0
    while(get_num_apps(yarn_host, "accepted,running") != 0):
        time.sleep(5)
    start_num_finished_apps = get_num_apps(yarn_host, "finished")
    print start_num_finished_apps
    start = time.time() * 1000
    
    rf = Algorithm("rf", 1, 64, 1)
    algorithms = []
    algorithms.append(rf)
    argslist = [jetty_host, "ModelSubmission", table, features, target, key_cols, algorithms]
    parallel_run(submit_model, argslist)
    
    time.sleep(3)
    
    rf = Algorithm("rf", 1, 64, 0)
    algorithms = []
    algorithms.append(rf)
    argslist = [jetty_host, "ModelSubmission", table, features, target, key_cols, algorithms]
    parallel_run(submit_model, argslist)

    while(get_num_apps(yarn_host, "finished") - start_num_finished_apps - len(customers) * 2 != 0):
        time.sleep(1)
        
    elapsed_time = time.time() * 1000 - start
    print elapsed_time
    time_list = [elapsed_time]
    directory = "ledp_preemption_submit"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + '/preemption_elapsed_time.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(["preemption_elapsed_time"])
        writer.writerow(time_list)
    
if __name__ == '__main__':
    main()
