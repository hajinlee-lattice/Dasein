__author__ = 'BWang'

import argparse,sys,json
sys.path.append("..")
"""
parser = argparse.ArgumentParser()
parser.add_argument('-func', '--function', dest = 'function_name', action = 'store', required = True, help = 'name of the function')
parser.add_argument('-t', '--tenant', dest = 'tenant', action = 'store', required = False, help = 'name of the tenant')
parser.add_argument('-db', '--database', dest = 'database', action = 'store', required = False, help = 'database connection string')
parser.add_argument('-k', '--key', dest = 'key', action = 'store', required = False, help = 'one time key')
parser.add_argument('-tk', '--token', dest = 'token', action = 'store', required = False, help = 'Access Token')
args = parser.parse_args()
"""
def updateConfigIni(tenantName,host,QueneName,SFDCPWD="",playName="",playType="",SFDCUser="",withModelingOnDataPlatform="TRUE"):
    with open("..\\config.ini") as ini:
        jsonIni=json.load(ini)
        if withModelingOnDataPlatform == "FALSE":
            jsonIni['withModelingOnDataPlatform']=withModelingOnDataPlatform
        jsonIni["tenantName"]=tenantName
        jsonIni["host"]=host
        jsonIni["QueneName"]=QueneName
        if playName:
            jsonIni["playName"]=playName
        if playType:
            jsonIni['playType']=playType
        if SFDCUser:
            jsonIni['SFDCUser']=SFDCUser
            if not SFDCPWD:
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
    parser.add_argument('-d', '--withModelingOnDataPlatform', dest = 'withModelingOnDataPlatform', action = 'store', required = False, help = 'whether need score with data platform')
    args = parser.parse_args()
    updateConfigIni(tenantName=args.tenantName,host=args.host,QueneName=args.queneName,SFDCUser=args.SFDCUser,SFDCPWD=args.SFDCPWD,playName=args.playName,playType=args.playType,withModelingOnDataPlatform=args.withModelingOnDataPlatform)