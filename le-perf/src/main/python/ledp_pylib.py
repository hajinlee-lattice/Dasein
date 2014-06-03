'''
Created on May 6, 2014

@author: hliu
'''
from dbcreds import DBCreds
from algorithm import Algorithm
import os, subprocess
        
def load(jetty_host, db_creds, table, key_cols, metadata_table, customer):
    os.system("java -cp '../../../target/dependency/*:../../../target/classes' com/latticeengines/perf/exposed/cli/ModelResourceCLI " 
              + "ledp load " + jetty_host + " -H " + db_creds.host + " -P " + str(db_creds.port) 
              + " -db " + db_creds.db_name + " -u " + db_creds.user + " -ps " + db_creds.passwd 
              + " -c " + customer + " -t " + table + " -kc " + key_cols + " -mt " + metadata_table)
    
def create_samples(jetty_host, table, training_percentage, num_samples, customer):
    os.system("java -cp '../../../target/dependency/*:../../../target/classes' com/latticeengines/perf/exposed/cli/ModelResourceCLI " 
              + "ledp createsamples " + jetty_host + " -c " + customer + " -t " + table + " -tp " + str(training_percentage) 
              + " -N " + str(num_samples))
   
def submit_model(jetty_host, model_name, table, target, key_cols, algorithms, metadata_table, customer):
    alg_props = ""
    for algorithm in algorithms:
        alg_props += " --alg -n " + algorithm.name + " -vc " + str(algorithm.virtual_cores) + " -m " + str(algorithm.memory) + " -p " + str(algorithm.priority)
    os.system("java -cp '../../../target/dependency/*:../../../target/classes' com/latticeengines/perf/exposed/cli/ModelResourceCLI " 
              + "ledp submitmodel " + model_name + " " + jetty_host + " -c " + customer + " -t " + table + " -T " + target 
              + " -kc " + key_cols + " -mt " + metadata_table + alg_props)

def get_num_apps(yarn_host, states):
    cmd = "java -cp '../../../target/dependency/*:../../../target/classes' com/latticeengines/perf/exposed/cli/ModelResourceCLI " + "ledp getfinishedappsnum " + yarn_host + " -s " + states
    p1 = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    return int(p1.stdout.read())
