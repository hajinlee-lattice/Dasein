from __future__ import print_function
import argparse
import os
import os.path
import subprocess
import filecmp
import sys
import string

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("scoring_harness_jar", help="Location of scoring harness jar file")
    parser.add_argument("scenarios_folder", help="Location of folder for scenarios to run")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.scoring_harness_jar):
        print("Must provide a scoring harness jar that exists. {0} does not exist".format(args.scoring_harness_jar), file=sys.stderr)
        exit(1)
    
    if not os.path.isdir(args.scenarios_folder):
        print("Must provide a scenarios folder that exists. {0} does not exist".format(args.scenarios_folder), file=sys.stder)
        exit(1)
    
    scenarios = []
    for entry in os.listdir(args.scenarios_folder):
        completePath = os.path.join(args.scenarios_folder, entry)
        if os.path.isdir(completePath):
            scenarios.append(Scenario(args.scoring_harness_jar, completePath))
    
    for scenario in scenarios:
        scenario.run()

    for scenario in scenarios:
        if not scenario.successful:
            print("One or more scenarios failed to run successfully.", file=sys.stderr)
            exit(1)
    

class Scenario(object):
    def __init__(self, scoringHarnessJar, scenarioFolder):
        self.__scoringHarnessJar = scoringHarnessJar
        self.__scenarioFolder = scenarioFolder
        self.__inputFile = os.path.join(self.__scenarioFolder, "input.json")
        self.__outputFile = os.path.join(self.__scenarioFolder, "output.txt")
        self.__expectedOutputFile = os.path.join(self.__scenarioFolder, "expectedoutput.txt")
        self.__scenarioName = os.path.basename(self.__scenarioFolder)
        self.successful = False        
                
    def run(self):
        print("[SCENARIO {0}]".format(self.__scenarioName))
        
        # Run scenario
        ret = subprocess.call(['java', '-jar', self.__scoringHarnessJar, self.__inputFile, self.__outputFile])
        
        # Check if process succeeded
        if ret != 0:
            print("FAILURE!  (Process terminated with code {0})".format(ret), file=sys.stderr)
            return

        # Check for output        
        if not os.path.exists(self.__outputFile):
            print("FAILURE!  (No output written to {0})".format(self.__outputFile), file=sys.stderr)
            return
        
        # Compare output
        if filecmp.cmp(self.__outputFile, self.__expectedOutputFile):
            self.successful = True
            print("SUCCESSS")
        else:
            print("FAILURE!", file=sys.stderr)
            print("Expected:", file=sys.stderr)
            print(self.getExpectedOutput(), file=sys.stderr)
            print("Received:")
            print(self.getReceivedOutput(), file=sys.stderr)

    def getExpectedOutput(self):
        with open(self.__expectedOutputFile) as f:
            lines = f.readlines()
            return string.join(lines)
    
    def getReceivedOutput(self):
        with open(self.__outputFile) as f:
            lines = f.readlines()
            return string.join(lines)
    
if __name__ == "__main__":
    sys.exit(main(sys.argv))