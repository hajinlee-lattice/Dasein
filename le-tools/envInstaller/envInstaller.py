#!/usr/local/bin/python
# coding: utf-8

# Installation script

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules

import argparse
#import os
import os.path
import time
import traceback
from collections import OrderedDict
from difflib import SequenceMatcher
from subprocess import PIPE
from subprocess import Popen

###############################################################################

# Global variables (do not remove from this file):
HADOOP = "hadoop-2.4.0.2.1.3.0-563"
SQOOP = "sqoop-1.4.4.2.1.3.0-563.bin__hadoop-2.4.0.2.1.3.0-563"
ECLIPSE = "eclipse-standard-kepler-SR2-linux-gtk-x86_64"
YARN_HOME = "%s/Tools/%s" % (os.getenv("HOME","~"), HADOOP)
EXTRA_LOGGING = False

###############################################################################
# All decoratable functions should be here. Before config.

def elog(s):
    if EXTRA_LOGGING:
        print(s)

def write_to_file(filename, updates):
    try:
        f = open(filename, "w+")
        if type(updates) == list:
            f.writelines(updates)
        else:
            f.write(updates)
        f.close()
    except IOError:
        e = traceback.format_exc()
        print("Unable to modify the file: %s" % filename)
        print("%s\n Exception details:\n%s\n%s" % (("~"*80), e, ("~"*80)))

def get_file_content(filename):
    content = []
    try:
        f = open(filename, "r+")
        content = f.readlines()
        f.close()
    except IOError:
        e = traceback.format_exc()
        print("Unable to modify the file: %s" % filename)
        print("%s\n Exception details:\n%s\n%s" % (("~"*80), e, ("~"*80)))
    return content

def add_file_header(filename, header):
    content = []
    if type(header) == list:
        content = header
    else:
        content.append(header) 
    content += get_file_content(filename)
    write_to_file(filename, content)

def add_file_footer(filename, footer):
    content = []
    content += get_file_content(filename)
    if type(footer) == list:
        content = content + footer
    else:
        content.append(footer) 
    write_to_file(filename, content)

def similarity_update(lines, update, precision, matching_type="max_ratio"):
    """
    Simple routine for making decisions on what to update in text:)
    Args:
      lines: Text to be updated
      update: Updated text
      precision: Minimum ration threshold
      matching_type: Type of matching used. Can be:
          max_ratio - (default) Simply use maximum ratio
          max_match_block_size - Biggest single substring match
          min_match_block_num - Minimum number of matching blocks
    """
    most_similar = {}
    scores_indexes = {}
    match_blocks = {}
    opcodes = {}
    recommendation = None
    # Validate each line in the file against what needs to be updated
    for i in range(len(lines)):
        s = SequenceMatcher(None, lines[i], update)
        ratio = s.ratio()
        if ratio >= precision:
            most_similar[ratio] = lines[i]
            scores_indexes[ratio] = i
            match_blocks[ratio] = s.get_matching_blocks()
            opcodes[ratio] = s.get_opcodes()
    # For debugging. Will print only if EXTRA_LOGGING = True
    #--------------------------------------------------------------------
    for key in sorted(most_similar.keys()):
        elog("%s : %s in position: %s" % (key, most_similar[key], scores_indexes[key]))
    elog(update)
    if most_similar.keys():
        elog("Recommend to update '%s' with '%s'" % (lines[scores_indexes[max(most_similar.keys())]], update))
    #-------------------------------------------------------------------------
    if most_similar.keys():
        elog("--- Using [%s] matching type---" % matching_type)
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
        if recommendation:
            #elog(recommendation)
            elog("Recommend to update '%s' with '%s'" % (recommendation[1], update))
            lines[recommendation[0]] = update
            #elog(lines)
    else:
        lines.append(update)
    return lines

def insert_update(filename, qualifying_lines, lines_to_insert):
    """
    Routine for inserting text into file based on preceding text
    Args:
      filename:         what file to update
      qualifying_lines: qualifying preceding text
      lines_to_insert:  lines to insert
    """
    lines = get_file_content(filename)
    # Make sure input is correctly typed.
    if type(qualifying_lines) != list:
        qualifying_lines = [qualifying_lines]
    if type(lines_to_insert) != list:
        lines_to_insert = [lines_to_insert]
    # If there are no qualifying lines, simply add the updates
    # if you need smart update you should use similarity_update() & update_file()
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
                # When all qualifying lines matched, do the smart insert
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
        # If we did not find any qualifying lines,
        # add both updates and qualifications at the end of the file
        if q == 0:
            lines = lines + qualifying_lines + lines_to_insert    
    write_to_file(filename, lines)

def update_file(filename, update_string, precision=0.3, match_type="max_ratio"):
    """
    Routine for updating files (add params, etc)
    Args:
      filename:        what file to update
      update_string:   what to write
      precision:       similarity precision
    """
    # Take care of *nix home directory
    if filename.startswith("~"):
        filename = os.path.expanduser(filename)
    # If such a file already exists, update the file
    if os.path.isfile(filename):
        lines = get_file_content(filename)
        new_lines = lines
        if type(update_string) == list:
            for line in update_string:
                new_lines = similarity_update(new_lines, line, precision, match_type)
        else:
            new_lines = similarity_update(new_lines, update_string, precision, match_type)
        # Re-write the content of the file
        #elog(new_lines)
        write_to_file(filename, new_lines)
    else:
        # If there is no file, simply add required strings to the new file
        write_to_file(filename, update_string)

###############################################################################

# Config Dictionary
config = OrderedDict()

# Detailed configs should be in the appropriate config files.
# General format should be:
# config["installation_type"] = {1:("cmd", "command line to execute"),
#                                2:("@py", [decoratable_function_name,
#                                           first_parameter,
#                                           second_parameter, etc])
#                               }
# You can combine configs if necessary:
# config["test"] =  dict([(i, config["mysql"][i]) for i in [5, 6] if i in config["mysql"]])
# new config will have command 5 and 6 from MySQL config
# config["test"] = {1: config["mysql"][3], 2: config["hadoop"][1]}
# new config will have 3rd cmd from MySQL and 1st cmd from HADOOP
###############################################################################

# Main (not decoratable in config dict) routines.
def install_updates(installation_type, log_file):
    """
    Main routine for controlling installation process
    Args:
      installation_type:    What to install
      log_file:             Name of the file to log the stderr and stdout
    """
    # Go through all pkgs in config dict 
    for pkg in config:
        # If installation type matches or set to "all"
        if installation_type == "all" or installation_type == pkg:
            # Make "test" is executed only when directly specified
            if installation_type == "all" and pkg == "test":
                continue
            print("%s\n Running installation of %s\n%s" % (("*"*80), pkg, ("*"*80)))
            # Execute every installation command
            for step in sorted(config[pkg].keys()):
                step_value = config[pkg][step]
                # Make sure the step value is Tuple
                if type(step_value) != tuple:
                    print("WARNING: Bad value in config, it MUST BE a tuple!!!")
                    print("Inst Type:%s, Step:%s, CMD:%s" % (pkg, step, step_value))
                    continue
                cmd_type = step_value[0]
                cmd = step_value[1]
                print ("---------------------")
                # Regular cmd
                if cmd_type == "cmd":
                    out, err = execute_command(cmd)
                    # Logging the STDERR and STDOUT
                    log_results(log_file, cmd, err, out)
                elif cmd_type == "cmd_from_dir":
                    if type(cmd) != list:
                        print("WARNING: Bad cmd_from_dir value in config, it MUST BE a list!!!")
                        print("Inst Type:%s, CMD Type:%s, CMD:%s" % (pkg, cmd_type, cmd))
                    else:
                        out, err = execute_command(cmd[0],cmd[1])
                        # Logging the STDERR and STDOUT
                        log_results(log_file, cmd, err, out)
                # Python decorated function
                elif cmd_type == "@py":
                    # Somebody messed up with "@py" config
                    if type(cmd) != list:
                        print("WARNING: Bad @py value in config, it MUST BE a list!!!")
                        print("Inst Type:%s, CMD Type:%s, CMD:%s" % (pkg, cmd_type, cmd))
                    else:
                        # Running just the decorated function
                        if len(cmd) == 1:
                            print "Running %s()" % cmd[0]
                            try:
                                cmd[0]()
                            except:
                                e = traceback.format_exc()
                                print("%s\n Exception details:\n%s\n%s" % (("~"*80), e, ("~"*80)))
                        # Running decorated function with parameters
                        else:
                            print "Running %s(%s)" % (cmd[0],cmd[1:])
                            try:
                                cmd[0](*cmd[1:])
                            except:
                                e = traceback.format_exc()
                                print(e)
                # Somebody messed up with config
                else:
                    print("WARNING: Bad command line type in config!!!")
                    print("Inst Type:%s, CMD Type:%s, CMD:%s" % (pkg, cmd_type, cmd))


def execute_command(cmd, from_dir=None):
    """
    Main routine for executing external program
    Args:
      cmd:      Command line for program execution
      log_file: Name of the file to log the stderr and stdout
    """

    if from_dir is None:
        from_dir = os.getcwd()
    # Set the timer
    start = time.time()

    # Run the external program
    print("Running: %s" % cmd)
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True, cwd=from_dir)
    out, err = p.communicate()

    elapsed = (time.time() - start)
    print ("Time of execution: %f seconds" % elapsed)

    # Report execution status as defined by requirements
    if p.returncode == 0:
        print ("Execution status: PASS")
    elif p.returncode is None:
        print ("Execution status: ERROR")
    else:
        print ("Execution status: FAIL")

    return out, err


def log_results(log_file, cmd, err, out):
    """
    Logging routine
    Args:
      log_file: Name of the file to log the stderr and stdout
      err:      STDERR of the command line
      out:      STDOUT of the command line
    """
    try:
        f = open (log_file, "a+")
        f.write("%s\n" % ("-*-*" * 20))
        f.write("Running: %s\n" % cmd)
        f.write("STDERR output:\n")
        f.write(err)
        f.write("\nSTDOUT output:\n")
        f.write(out)
        f.write("\nEnd of log\n")
        f.write("%s\n" % ("-*-*" * 20))
        f.close()
        print("Execution log is saved to %s" % os.path.abspath(log_file))
    except IOError:
        e = traceback.format_exc()
        print("Unable to save the log file")
        print("%s\n Exception details:\n%s\n%s" % (("~"*80), e, ("~"*80)))

###############################################################################

def main():
    """
    Here we have processing for installation_type and log_file args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", help="installation config")
    parser.add_argument("installation_type", help="What to install")
    parser.add_argument("logfile", help="Log file for output")
    args = parser.parse_args()
    
    # Load the config
    execfile(args.config_file, globals())

    try:
        # Run the main routine
        install_updates(args.installation_type, args.logfile)
    except ValueError:
        # Popen throws ValueError exception when it fails.
        e = traceback.format_exc()
        print ("Execution status: ERROR")
        print("%s\n Exception details:\n%s\n%s" % (("~"*80), e, ("~"*80)))

if __name__ == "__main__":
    main()