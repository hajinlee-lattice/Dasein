'''
Run this script, user can init the Dante configuration for DB and Package
'''
__author__ = 'nxu'

import argparse
from ConfigParser import SafeConfigParser
import logging


def setDanteDBProperties():

    ''' Parsing the properties from arguments '''
    print '==='
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--Server', dest = 'server', action = 'store', required = True, help = 'Server for DB')
    parser.add_argument('-U', '--username', dest = 'user', action = 'store', required = True, help = 'name of the DB user')
    parser.add_argument('-P', '--password', dest = 'pwd', action = 'store', required = True, help = 'password for DB user')
    parser.add_argument('-D', '--DataBase', dest = 'db_name', action = 'store', required = True, help = 'Dante DB Name')
    args = parser.parse_args()

    ''' Setting up properties in config.ini file... '''
    logging.info("Setting up properties in config.ini file...")
    configFile = '.\Dante\config.ini'
    configParser = SafeConfigParser()
    configParser.read(configFile)
    configParser.set('Dante_DB', 'Dante_DB_Server', args.server)
    configParser.set('Dante_DB', 'Dante_DB_User', args.user)
    configParser.set('Dante_DB', 'Dante_DB_PWD', args.pwd)
    configParser.set('Dante_DB', 'Dante_DB_Name', args.db_name)
    with open(configFile, 'wb') as configfile:
        configParser.write(configfile)


if __name__ == '__main__':
    print 'start call setproperoties'
    setDanteDBProperties()

