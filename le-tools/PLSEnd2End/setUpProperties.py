'''
Created on Mar 25, 2015

@author: smeng
'''


import argparse
from ConfigParser import SafeConfigParser
import logging


def setProperties():
    
    ''' Parsing the properties from arguments '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tenant', dest = 'tenant', action = 'store', required = True, help = 'name of the tenant')
    parser.add_argument('-t2', '--tenant2', dest = 'tenant_2', action = 'store', required = True, help = 'name of the second tenant')
    parser.add_argument('-s', '--server', dest = 'pls_server', action = 'store', required = True, help = 'host name of the PLS server')
    parser.add_argument('-d', '--directory', dest = 'install_dir', action = 'store', required = True, help = 'PLS installed directory on the PLS server')
    args = parser.parse_args()
    print "After parse: " + args.install_dir


    ''' Setting up properties in config.ini file... '''    
    logging.info("Setting up properties in config.ini file...")
    configFile = 'config.ini'
    configParser = SafeConfigParser()
    configParser.read(configFile)
    configParser.set('BuildInfo', 'tenant', args.tenant)
    configParser.set('BuildInfo', 'tenant_2', args.tenant_2)
    configParser.set('BuildInfo', 'pls_server', args.pls_server)
    configParser.set('BuildInfo', 'install_dir', args.install_dir)
    with open(configFile, 'wb') as configfile:
        configParser.write(configfile)


if __name__ == '__main__':
    setProperties()