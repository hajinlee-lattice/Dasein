'''
This script is to set configuration for leads and accounts package. The Dante-Service-URL is from config.ini
And the config.ini can be initialize by script SetipProperties.py
'''
__author__ = 'nxu'

from Property import DanteEnvironments
from Operations.DantePageHelper import DantePageHelper

if __name__ == '__main__':
    print '=====start to setUp Leads and Accounts Configuraion======'
    dp_c=DantePageHelper()
    dp_c.SetDanteServiceURL('Lead',DanteEnvironments.Sales_Force_DT_service,True,True,True)
    dp_c.SetDanteServiceURL('Account',DanteEnvironments.Sales_Force_DT_service,True,True)
    print '=====End to setup leads and accounts configuration'




