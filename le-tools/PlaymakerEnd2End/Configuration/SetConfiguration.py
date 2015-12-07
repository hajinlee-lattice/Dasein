__author__ = 'BWang'

import argparse,json


def updateConfigIni(tenantName,host,QueneName,SFDCPWD,playName,playType,SFDCUser,driverType,withModelingOnDataPlatform="TRUE"):
    with open(".\\PlaymakerEnd2End\\config.ini") as ini:
        jsonIni=json.load(ini)
        jsonIni['withModelingOnDataPlatform']=withModelingOnDataPlatform
        jsonIni["tenantName"]=tenantName
        jsonIni["host"]=host
        jsonIni["QueneName"]=QueneName
        jsonIni["driverType"]=driverType
        jsonIni['playType']=playType
        if playName!= "default":
            jsonIni["playName"]=playName
        if SFDCUser!= "default":
            jsonIni['SFDCUser']=SFDCUser
            if SFDCPWD == "default":
                print("You have chanded SFDC user,please also update the password!")
                return
            else:
                jsonIni['SFDCPWD']=SFDCPWD
    with open("..\\config.ini",mode='w') as ini:
        ini.write(json.dumps(jsonIni))
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-tn', '--tenantName', dest = 'tenantName', action = 'store', required = True, help = 'name of the tenant')
    parser.add_argument('-host', '--host', dest = 'host', action = 'store', required = True, help = 'host of the target machine')
    parser.add_argument('-qn', '--queneName', dest = 'queneName', action = 'store', required = True, help = 'QueneName of the host')
    parser.add_argument('-su', '--SFDCUser', dest = 'SFDCUser', action = 'store', required = False, help = 'user to login salesforce')
    parser.add_argument('-sp', '--SFDCPWD', dest = 'SFDCPWD', action = 'store', required = False, help = 'password of the user to login salesforce')
    parser.add_argument('-pn', '--playName', dest = 'playName', action = 'store', required = False, help = 'the name of play you want to create')
    parser.add_argument('-pt', '--playType', dest = 'playType', action = 'store', required = False, help = 'which type of play you want to create')
    parser.add_argument('-dt', '--driverType', dest = 'driverType', action = 'store', required = False, help = 'which browser you want to use')
    parser.add_argument('-d', '--withModelingOnDataPlatform', dest = 'withModelingOnDataPlatform', action = 'store', required = False, help = 'whether need score with data platform')
    args = parser.parse_args()
    updateConfigIni(tenantName=args.tenantName,host=args.host,QueneName=args.queneName,SFDCUser=args.SFDCUser,SFDCPWD=args.SFDCPWD,playName=args.playName,playType=args.playType,driverType=args.driverType,withModelingOnDataPlatform=args.withModelingOnDataPlatform)