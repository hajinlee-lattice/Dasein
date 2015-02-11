from __future__ import print_function
from abc import abstractmethod
import abc
import argparse
import os
import os.path
import subprocess
import filecmp
import sys
import string
import uuid
import json

class Operation(object):
    __metaclass__ = abc.ABCMeta
    
    def __init__(self, offset_ms, external_id):
        self._offset_ms = offset_ms
        self._external_id = external_id
    
    @abstractmethod
    def get_structure(self):
        raise Exception("abstract")
        
    def get_offset(self):
        return self._offset_ms

class WriteLeadOperation(Operation):
    def __init__(self, offset_ms, external_id):
        super(WriteLeadOperation, self).__init__(offset_ms, external_id)
    
    def get_structure(self):
        structure = {
             "@type":"write", 
             "offsetMilliseconds":self._offset_ms, 
             "externalId":self._external_id}
        structure["object"] = dict()
        
        return structure

class ReadLeadScoreOperation(Operation):
    def __init__(self, offset_ms, external_id, additional_fields=None):
        super(ReadLeadScoreOperation, self).__init__(offset_ms, external_id)
        self.__additional_fields = additional_fields
    
    def get_structure(self):
        structure = {
             "@type":"readscore", 
             "offsetMilliseconds":self._offset_ms, 
             "externalId":self._external_id}
        
        return structure

def generate_external_id():
    guid = uuid.uuid1()
    return guid.hex + "@lattice-engines.com"

def main(argv):
    parser = argparse.ArgumentParser(argv)
    parser.add_argument("scenario_name", help = "The name of the scenario folder to generate in the current directory.")
    parser.add_argument("-w", "--writes", type = int, dest = "writes", help = "Number of write operations to perform per hour.  Defaults to 60.")
    parser.add_argument("-s", "--read-sla", type = int, dest = "read_sla", help = "The number of milliseconds to wait after a write before performing a read.  Defaults to 1000.")
    parser.add_argument("-l", "--scenario-length", type = int, dest = "scenario_length", help = "The scenario length in minutes.  Defaults to 15.  Note that the scenario may last a bit longer or shorter than this amount of time.")

    args = parser.parse_args()
        
    # Set defaults
    if args.writes is None:
        args.writes = 60
    if args.read_sla is None:
        args.read_sla = 1000
    if args.scenario_length is None:
        args.scenario_length = 15

    # TODO Validate parameters

    scenario_length_ms = args.scenario_length * 1000
    write_cadence_ms = scenario_length_ms / (args.writes * args.scenario_length / 60)

    try:
        os.mkdir(args.scenario_name)
        input_file = open(os.path.join(args.scenario_name, "input.json"), 'w')
    except Exception as e:
        print("Failed to generate scenario directory {0}: {1}".format(args.scenario_name, str(e)), file=sys.stderr)
        exit(1) 

    # Generate operation array
    operations = []
      
    offset = 0
    while offset < scenario_length_ms - args.read_sla:
        external_id = generate_external_id()
        print(external_id)

        operations.append(WriteLeadOperation(offset, external_id))
        operations.append(ReadLeadScoreOperation(offset + args.read_sla, external_id, None))
        offset += write_cadence_ms
    
    # Sort
    operations.sort(key = Operation.get_offset)

    input_array = []
    for operation in operations:
        input_array.append(operation.get_structure())
    print(json.dumps(input_array, indent=4), file=input_file)

if __name__ == "__main__":
    sys.exit(main(sys.argv))