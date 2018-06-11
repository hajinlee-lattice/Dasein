from argparse import ArgumentParser
import os
import sys
import fileinput
import shutil
import subprocess

# Assumes this is being run from Trunk\Application\installer\app
def main(argv):
    parser = ArgumentParser(add_help = False)
    parser.add_argument("version")
    arguments = parser.parse_args(argv[1:])
    
    # Rename production.js to production_{version}.js for caching purposes
    os.rename(os.path.normpath("production.js"), os.path.normpath("production_" + arguments.version + ".js"))
    
    for line in fileinput.FileInput(os.path.normpath("../index.aspx"),inplace=1):
        sys.stdout.write(line.replace("@@versionString", arguments.version))
    
    # This needs to be after the index.aspx replacement because the pathing was getting screwed up after this line runs...Not sure why
    os.rename(os.path.normpath("../assets/styles/production.css"), os.path.normpath("../assets/styles/production_" + arguments.version + ".css"))

def runShell(command):
    process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
    outputData, errorData = process.communicate()

    if process.returncode != 0:
        raise InvocationError(command, process.returncode, errorData)

class InvocationError(Exception):
    __message = ""

    def __init__(self, command, code, message):
        self.command = command
        self.code = code
        self.__message = message
        
    def __str__(self):
        return "InvocationError: [" + self.command + "] exited with code [" + str(self.code) + "] message: [" + self.__message + "]"

if __name__ == "__main__":
    sys.exit(main(sys.argv))