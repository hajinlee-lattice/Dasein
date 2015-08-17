'''
Run this script, user can init the Dante configuration for DB and Package
'''
__author__ = 'nxu'

import argparse
from ConfigParser import SafeConfigParser
import logging


def setProperties():

    ''' Parsing the properties from arguments '''
    print '==='
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--Server', dest = 'server', action = 'store', required = True, help = 'Server for DB')
    parser.add_argument('-U', '--username', dest = 'user', action = 'store', required = True, help = 'name of the DB user')
    parser.add_argument('-P', '--password', dest = 'pwd', action = 'store', required = True, help = 'password for DB user')
    parser.add_argument('-D', '--DataBase', dest = 'db_name', action = 'store', required = True, help = 'Dante DB Name')
    parser.add_argument('-L', '--salesforceurl', dest = 'sf_url', action = 'store', required = True, help = 'log in url for sales force')
    parser.add_argument('-SU', '--salesforceuser', dest = 'sf_user', action = 'store', required = True, help = 'log in user for sales force')
    parser.add_argument('-SP', '--salesforcepwd', dest = 'sf_pwd', action = 'store', required = True, help = 'log in password for sales force')
    parser.add_argument('-SS', '--salesforceDanteService', dest = 'sf_dt_service', action = 'store', required = True, help = 'DT service for sales force')
    args = parser.parse_args()

    ''' Setting up properties in config.ini file... '''
    logging.info("Setting up properties in config.ini file...")
    configFile = 'config.ini'
    configParser = SafeConfigParser()
    configParser.read(configFile)
    configParser.set('Dante_DB', 'Dante_DB_Server', args.server)
    configParser.set('Dante_DB', 'Dante_DB_User', args.user)
    configParser.set('Dante_DB', 'Dante_DB_PWD', args.pwd)
    configParser.set('Dante_DB', 'Dante_DB_Name', args.db_name)
    configParser.set('Sales_Force', 'Sales_Force_URL', args.sf_url)
    configParser.set('Sales_Force', 'Sales_Force_User', args.sf_user)
    configParser.set('Sales_Force', 'Sales_Force_PWD', args.sf_pwd)
    configParser.set('Sales_Force', 'Sales_Force_DT_service', args.sf_dt_service)
    with open(configFile, 'wb') as configfile:
        configParser.write(configfile)


if __name__ == '__main__':
    print 'start call setproperoties'
    setProperties()

