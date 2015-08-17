'''
@author: nxu
'''


import argparse
import os

def ExportDanteSampleDate():
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--Server', dest = 'server', action = 'store', required = True, help = 'Server for DB')
    parser.add_argument('-U', '--username', dest = 'user', action = 'store', required = True, help = 'name of the DB user')
    parser.add_argument('-P', '--password', dest = 'pwd', action = 'store', required = True, help = 'password for DB user')
    parser.add_argument('-i', '--sql_file_path', dest = 'sql_file', action = 'store', required = True, help = 'The path for sql file need implemented')
    parser.add_argument('-rd', '--Replace_DB_Name', dest = 'replace_db_name', action = 'store', required = True, help = 'DB Name will implement sql')
    parser.add_argument('-rc', '--Replace_customer_ID', dest = 'replace_customer_ID', action = 'store', required = True, help = 'New customer id for Dante')
    args = parser.parse_args()

    newfilepath=CreateFinalImplementsqlfile(args.sql_file,args.replace_db_name,args.replace_customer_ID)
    if os.path.isfile(newfilepath):
        command="sqlcmd.exe -S "+args.server+" -U "+args.user+" -P "+args.pwd+" -i "+newfilepath
        print command
        result=os.system(command)
        #print result
        print "Completed Dante sample Data in DB "+args.replace_db_name+" in server "+ args.server
        os.remove(newfilepath)
        print "Implement temp file have been deleted."
    else:
        print "Implement sql file can't be created successfully!"


def CreateFinalImplementsqlfile(path_sql,new_DB_Name,new_Customer_ID):
    try:
        if os.path.isfile(path_sql):
            new_file_path=path_sql+'.temp.sql'
            f_out=open(path_sql,'r')
            content=f_out.read()
            new_usedb="USE ["+new_DB_Name+"]"
            content=content.replace("USE [DateBase_Name_TEMP]",new_usedb)
            new_set_cid="set @Custmer_ID='"+new_Customer_ID+"'"
            content=content.replace("set @Custmer_ID='[Defalut_Customer_ID]'",new_set_cid)
            f_in=open(new_file_path,'w')
            f_in.write(content)
            return new_file_path
        else:
            print "File not exist or valid in "+path_sql
    except Exception,e:
        print "IO issue"
        print e.message
    finally:
        f_in.close()
        f_out.close()

if __name__ == '__main__':
    ExportDanteSampleDate()