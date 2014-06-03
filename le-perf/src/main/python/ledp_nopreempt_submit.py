'''
Created on May 13, 2014

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
    print "The num of already started jobs is " + str(start_num_finished_apps)
    start = time.time() * 1000
    
    rf = Algorithm("rf", 1, 64, 0)
    algorithms = []
    algorithms.append(rf)
    argslist = [jetty_host, "ModelSubmission", table, target, key_cols, algorithms, metadata_table]
    num_apps = len(customers)
    MAX_THREADS = num_apps + 1
    parallel_run(submit_model, argslist, MAX_THREADS, True)

    apps_running = 1
    while(apps_running != 0):
        time.sleep(1)
        apps_running = get_num_apps(yarn_host, "finished") - start_num_finished_apps - len(customers)
        print "Still waiting for " + str(apps_running) + " number of jobs to complete!"
        
    elapsed_time = time.time() * 1000 - start
    print "Elapsed time is " + str(elapsed_time)
    time_list = [elapsed_time]
    directory = "ledp_nopreemption_submit"
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + '/nopreemption_elapsed_time.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(["nopreemption_elapsed_time"])
        writer.writerow(time_list)
        

if __name__ == '__main__':
    main()
