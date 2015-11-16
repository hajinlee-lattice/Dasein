'''
Run this script, user can init the Dante configuration for DB and Package
'''
__author__ = 'nxu'

import argparse
from ConfigParser import SafeConfigParser
import logging


def setDanteSFECProperties():

    ''' Parsing the properties from arguments '''
    print '==='
    parser = argparse.ArgumentParser()
    parser.add_argument('-UL', '--salesforceuserforLead', dest = 'sf_user_lead', action = 'store', required = True, help = 'log in user for sales force')
    parser.add_argument('-PL', '--salesforcepwdForLead', dest = 'sf_pwd_lead', action = 'store', required = True, help = 'log in password for sales force')
    parser.add_argument('-SL', '--salesforceDanteServiceForLead', dest = 'sf_dt_service_lead', action = 'store', required = True, help = 'DT service for sales force')
    parser.add_argument('-UA', '--salesforceuserforAccount', dest = 'sf_user_account', action = 'store', required = True, help = 'log in user for sales force')
    parser.add_argument('-PA', '--salesforcepwdForAccount', dest = 'sf_pwd_account', action = 'store', required = True, help = 'log in password for sales force')
    parser.add_argument('-SA', '--salesforceDanteServiceForAccount', dest = 'sf_dt_service_account', action = 'store', required = True, help = 'DT service for sales force')
    parser.add_argument('-BT', '--Browser Type to use', dest = 'sf_bt', action = 'store', required = False, help = 'Browser to automation e.g ff chrome')
    #parser.add_argument('-PU', '--PROD Dante URL', dest = 'sf_prod_url', action = 'store', required = False, help = 'DT service for sales force')
    args = parser.parse_args()

    ''' Setting up properties in config.ini file... '''
    logging.info("Setting up properties in config.ini file...")
    configFile = 'config.ini'
    configParser = SafeConfigParser()
    configParser.read(configFile)
    configParser.set('Sales_Force', 'Sales_Force_URL', args.sf_url)
    configParser.set('Sales_Force', 'sales_force_user_lead', args.sf_user_lead)
    configParser.set('Sales_Force', 'sales_force_pwd_lead', args.sf_pwd_lead)
    configParser.set('Sales_Force', 'sales_force_dt_service_lead', args.sf_dt_service_lead)
    configParser.set('Sales_Force', 'sales_force_dt_service_account', args.sf_dt_service_account)
    configParser.set('Sales_Force', 'browser_type', args.sf_bt)
    with open(configFile, 'wb') as configfile:
        configParser.write(configfile)


if __name__ == '__main__':
    print 'start call setproperoties'
    setDanteSFECProperties()

