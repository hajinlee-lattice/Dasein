__author__ = 'nxu'

from ConfigParser import SafeConfigParser

class DanteEnvironments(object):

    # parser that read configuration properties from config.ini file
    parser = SafeConfigParser()
    parser.read('config.ini')

    Prod_Dante_Server = parser.get('Sales_Force', 'prod_dante_serer');

    # properties defination
    # Configuration for Dante DB
    Dante_DB_Server_Server = parser.get('Dante_DB', 'Dante_DB_Server')
    Dante_DB_User = parser.get('Dante_DB', 'Dante_DB_User')
    Dante_DB_PWD = parser.get('Dante_DB', 'Dante_DB_PWD')
    Dante_DB_Name = parser.get('Dante_DB', 'Dante_DB_Name')
    # Configuration for sales force
    Sales_Force_URL = parser.get('Sales_Force', 'Sales_Force_URL')
    Sales_Force_User = parser.get('Sales_Force', 'Sales_Force_User')
    Sales_Force_PWD = parser.get('Sales_Force', 'Sales_Force_PWD')
    Sales_Force_DT_service = parser.get('Sales_Force', 'Sales_Force_DT_service')
    Browser_Type = parser.get('Sales_Force', 'Browser_Type')

    PROD_Dante_URL = 'https://' + Prod_Dante_Server + '?sin=DemoSession&serverurl=https%3A%2F%2Flbi.na31.visual.force.com%2Fservices%2FSoap%2Fu%2F34.0%2F00D37000000Pd2mEAC&Directory=salesforce&userlink=00537000000Rji5AAC&Recommendation=99&HasSalesprism=false&CustomSettings={%22SupportEmail%22%3A%22noreply%40salesforce.com%22%2C%22ShowScore%22%3A%20false%2C%22ShowLift%22%3A%20false%2C%22NoPlaysMessage%22%3A%22No%20Plays%20Found.%22%2C%22NoDataMessage%22%3A%22No%20Data%20Found.%22%2C%22hideNavigation%22%3A%20true%2C%22DefaultTab%22%3A%22TalkingPoints%22}'
    Conf_file_Dante_Page = '.\Config\DantePage.yml'

