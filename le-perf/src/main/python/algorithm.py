'''
Created on May 6, 2014

@author: hliu
'''

class Algorithm(object):
    '''
    classdocs
    '''

    def __init__(self, name, virtual_cores, memory, priority):
        self.name = name
        self.virtual_cores = virtual_cores
        self.memory = memory
        self.priority = priority