__author__ = 'nxu'

from ConfigParser import SafeConfigParser

class DanteEnvironments(object):

    # parser that read configuration properties from config.ini file
    parser = SafeConfigParser()
    parser.read('config.ini')

    #properties defination
    #Configuration for Dante DB
    Dante_DB_Server_Server=parser.get('Dante_DB', 'Dante_DB_Server')
    Dante_DB_User=parser.get('Dante_DB', 'Dante_DB_User')
    Dante_DB_PWD=parser.get('Dante_DB', 'Dante_DB_PWD')
    Dante_DB_Name=parser.get('Dante_DB', 'Dante_DB_Name')
    #Configuration for sales force
    Sales_Force_URL=parser.get('Sales_Force', 'Sales_Force_URL')
    Sales_Force_User=parser.get('Sales_Force', 'Sales_Force_User')
    Sales_Force_PWD=parser.get('Sales_Force', 'Sales_Force_PWD')
    Sales_Force_DT_service=parser.get('Sales_Force', 'Sales_Force_DT_service')

    Conf_file_Dante_Page='.\Config\DantePage.yml'
