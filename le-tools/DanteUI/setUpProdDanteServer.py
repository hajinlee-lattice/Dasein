'''
Created on Nov 2, 2015

@author: smeng
'''

import argparse
from ConfigParser import SafeConfigParser
import logging


def setProperties():

    ''' Parsing the properties from arguments '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', dest='server', action='store', required=True, help='prod dante server address')
    args = parser.parse_args()


    ''' Setting up properties in config.ini file... '''
    logging.info("Setting up properties in config.ini file...")
    configFile = '.\DanteUI\config.ini'
    configParser = SafeConfigParser()
    configParser.read(configFile)
    configParser.set('Sales_Force', 'prod_dante_server', args.server)
    with open(configFile, 'wb') as configfile:
        configParser.write(configfile)


if __name__ == '__main__':
    setProperties()
