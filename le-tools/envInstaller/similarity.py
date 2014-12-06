#!/usr/local/bin/python
# coding: utf-8

# Script for testing the success of external program execution

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules
'''
import argparse
import os.path
import time
import traceback
from collections import OrderedDict
from subprocess import PIPE
from subprocess import Popen
'''
from difflib import SequenceMatcher
import pprint

TEST_LIST = ["export PYTHON_HOME=/usr/bin/python",
"export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64",
"export JAVA_LIB=/usr/lib/x86_64-linux-gnu/jni",
"export HADOOP_HOME=$HOME/Tools/hadoop-2.4.0.2.1.3.0-563",
"export HADOOP_MAPRED_HOME=$HOME/Tools/hadoop-2.4.0.2.1.3.0-563",
"export HADOOP_COMMON_HOME=$HOME/Tools/hadoop-2.4.0.2.1.3.0-563",
"export HADOOP_HDFS_HOME=$HOME/Tools/hadoop-2.4.0.2.1.3.0-563",
"export HADOOP_YARN_HOME=$HOME/Tools/hadoop-2.4.0.2.1.3.0-563",
"export HADOOP_CONF_DIR=$HOME/Tools/hadoop-2.4.0.2.1.3.0-563/etc/hadoop",
"export YARN_HOME=$HADOOP_HOME",
"export SQOOP_HOME=$HOME/Tools/sqoop-1.4.4.2.1.3.0-563.bin__hadoop-2.4.0.2.1.3.0-563",
"export PATH=$PATH:$HADOOP_HOME/bin",
"export PATH=$PATH:$HADOOP_HOME/sbin",
"export PATH=$PATH:$SQOOP_HOME/bin",
"export PATH=$JAVA_HOME/bin:$PATH",
"export PATH=$JAVA_LIB/bin:$PATH",
"export M2_REPO=~/.m2/repository",
"export PATH=$PATH:$HADOOP_HOME/test/bin",
"<value>file:/home/ivinnichenko/yarn/yarn_data/hdfs/namenode</value>",
"<value>file:/home/ivinnichenko/yarn/yarn_data/hdfs/datanode</value>",
"LOAD DATA INFILE '/home/hliu/workspace/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_train.dat'",
"LOAD DATA INFILE '/home/hliu/workspace/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_test.dat'"
]

def similarity_update(lines, update, precision, matching_type="max_ratio"):
    most_similar = {}
    scores_indexes = {}
    match_blocks = {}
    opcodes = {}
    recommendation = None
    # Validate each line in the file against what needs to be updated
    for i in range(len(lines)):
        s = SequenceMatcher(None, lines[i], update)
        ratio = s.ratio()
        #print ("'%s' vs '%s'\n ration is: %f" % (lines[i], update, ratio))
        if ratio >= precision:
            most_similar[ratio] = lines[i]
            scores_indexes[ratio] = i
            match_blocks[ratio] = s.get_matching_blocks()
            opcodes[ratio] = s.get_opcodes()
            
    
    for key in sorted(most_similar.keys()):
        print("%s : %s in position: %s" % (key, most_similar[key], scores_indexes[key]))
        """
        #max([x for (x,y) in l if y==2])
        #print (max([i2-i1 for tag, i1, i2, j1, j2 in opcodes[key] if tag == "equal"]))
        for tag, i1, i2, j1, j2 in opcodes[key]:
            if tag == "equal" and (len(most_similar[key]) - i2 == 0) and (len(update) - j2 == 0):
                #print len(most_similar[key]) - i2, len(update) - j2
                print ("%7s a[%d:%d] (%s) b[%d:%d] (%s)" %
                       (tag, i1, i2, most_similar[key][i1:i2], j1, j2, update[j1:j2]))
        
        """
        '''
        for block in match_blocks[key]:
            i,j,b = block
            print "a[%d] and b[%d] match for %d elements" % block
            print "%s == %s" % (most_similar[key][i:i+b],update[j:j+b])
        '''
    print(update)
    
    if most_similar.keys():
        print("--- Using [%s] matching type---" % matching_type)
        if matching_type == "max_ratio":
            recommendation = (scores_indexes[max(most_similar.keys())],
                              lines[scores_indexes[max(most_similar.keys())]])
        elif matching_type == "max_match_block_size":
            best_candidate = (0, 0)
            for key in sorted(most_similar.keys()):
                for block in match_blocks[key]:
                    if block[2] > best_candidate[1]:
                        best_candidate = (key, best_candidate[1])
            recommendation = (scores_indexes[best_candidate[0]],
                              lines[scores_indexes[best_candidate[0]]])
        elif matching_type == "min_match_block_num":
            best_candidate = (0, 10000)
            for key in sorted(most_similar.keys()):
                if len(match_blocks[key]) < best_candidate[1]:
                    print match_blocks[key]
                    best_candidate = (key, len(match_blocks[key]))
            recommendation = (scores_indexes[best_candidate[0]],
                              lines[scores_indexes[best_candidate[0]]])
        elif matching_type == "max_last_equal":
            best_candidate = (0, 0)
            for key in sorted(most_similar.keys()):
                for tag, i1, i2, j1, j2 in opcodes[key]:
                    if (tag == "equal" and 
                       (len(most_similar[key]) - i2 == 0) and 
                       (len(update) - j2 == 0) and
                       best_candidate[1] < i2 - i1):
                        best_candidate = (key, i2 - i1)
            recommendation = (scores_indexes[best_candidate[0]],
                              lines[scores_indexes[best_candidate[0]]])
        """                
        """
        if recommendation:
            print("Recommend to update '%s' with '%s'" % (recommendation[1], update))
            lines[recommendation[0]] = update
    else:
        lines.append(update)
    return lines

'''
def similarity_update(lines, update, precision):
    new_lines = []
    # Validate each line in the file against what needs to be updated
    for i in range(len(lines)):
        s = SequenceMatcher(None, lines[i], update)
        if s.ratio() >= precision:
            new_lines.append(update)
        else:
            new_lines.append(lines[i])
    return new_lines
'''
INSERT = [
"# remote nodes.",
"",
"# The java implementation to use.",
"export JAVA_HOME=${JAVA_HOME}",
"export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64",
"test",
"",
"# The jsvc implementation to use. Jsvc is required to run secure datanodes.",
"#export JSVC_HOME=${JSVC_HOME}"
]

QL = ["# The java implementation to use.",
"export JAVA_HOME=${JAVA_HOME}"]

L = ["export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64",#"test"
     ]

def insert_update(lines, qualifying_lines, lines_to_insert):
    if type(qualifying_lines) != list:
        qualifying_lines = [qualifying_lines]
    if type(lines_to_insert) != list:
        lines_to_insert = [lines_to_insert]
    if len(qualifying_lines) == 0:
        lines = lines + qualifying_lines + lines_to_insert
    else:
        q = 0
        limit = len(qualifying_lines) - 1
        qualified = False
        for i in range(len(lines)):
            if q > 0 and not qualified:
                q = 0
            print lines[i]
            print i,q,qualified
            if lines[i] == qualifying_lines[q]:
                qualified = True
                if q+1 > limit:
                    if len(lines[i+1:]) >= len(lines_to_insert):
                        subset = lines[i+1:len(lines_to_insert) + i+1]
                        # Check if updated lines already there
                        if subset == lines_to_insert:
                            break
                        # Check if updated lines already there, but in incorrect order
                        elif sorted(subset) == sorted(lines_to_insert):
                            lines[i+1:len(lines_to_insert) + i+1] = lines_to_insert
                            break
                        # Check if updated lines already there, but not all of them
                        else:
                            add_list = []
                            for l in range(len(subset)):
                                if subset[l] != lines_to_insert[l]:
                                    add_list.append(lines_to_insert[l])
                            lines = lines[:i+1] + add_list + lines[i+1:]
                            break
                    lines = lines[:i+1] + lines_to_insert + lines[i+1:]
                    break
                q += 1
            else:
                qualified = False
        if q == 0:
            lines = lines + qualifying_lines + lines_to_insert
    return lines

def simple_test_func():
    print "AhA!!!!"

Config = {5:6}
HOME = "Test"
#execfile("/home/ivinnichenko/Code/config.py")

def main():
    """
    Here we have processing for installation_type and log_file args
    """
    execfile("/home/ivinnichenko/Code/config.py", globals())
    print Config
    Config[2]()
    #similarity_update(["export JAVA_HOME=${JAVA_HOME}"], "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64", 0.3)
    similarity_update(TEST_LIST,
                      "LOAD DATA INFILE '/home/ivinnichenko/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_train.dat'",
        0.7, "max_last_equal")
    similarity_update(TEST_LIST,
                      "LOAD DATA INFILE '/home/ivinnichenko/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_test.dat'",
        0.7, "max_last_equal")
    '''
    pprint.pprint(insert_update(INSERT, QL, L))
    
    for line in TEST_LIST:
        similarity_update(TEST_LIST, line, 0.3)
        print("*" * 80)
    similarity_update(TEST_LIST, "export PATH=$PATH:$HADOOP_HOME/bin", 0.7, "min_match_block_num")
    #similarity_update(TEST_LIST, "export PATH=$PATH:$HADOOP_HOME/bin", 0.7, "max_match_block_size")
    #similarity_update(TEST_LIST, "export PATH=$PATH:$HADOOP_HOME/bin", 0.7, "max_ratio")
    
    similarity_update(TEST_LIST,
        "<value>file:/home/ivinnichenko/Tools/hadoop-2.4.0.2.1.3.0-563/yarn/yarn_data/hdfs/datanode</value>",
        0.7, "min_match_block_num")
    similarity_update(TEST_LIST,
        "<value>file:/home/ivinnichenko/Tools/hadoop-2.4.0.2.1.3.0-563/yarn/yarn_data/hdfs/namenode</value>",
        0.7, "min_match_block_num")
    '''
    

if __name__ == "__main__":
    main()