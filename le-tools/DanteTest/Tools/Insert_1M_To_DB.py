'''
@author: nxu
'''

import pyodbc
import os
import argparse

class CreateDanteSampleData(object):
    
    def __init__(self,server_s='10.41.1.87\SQL2012STD',DataBase_s='DT_ADEDTBDd720133nQ280105n154',server_t='10.41.1.87\SQL2012STD',DataBase_t='DT_ADEDTBDd720133nQ280105n154',User='dataloader_user',Password='password'):
        self.conn_info_source="DRIVER={SQL Server};Server="+server_s+";DATABASE="+DataBase_s+";UID="+User+";PWD="+Password
        self.conn_info_target="DRIVER={SQL Server};Server="+server_t+";DATABASE="+DataBase_t+";UID="+User+";PWD="+Password
        #self.file_LeadCache="..\Data\sql_LeadCache.sql"
        #self.file_AccountCache="..\Data\sql_AccountCache.sql"
        #self.file_FECModelSummary="..\Data\sql_FECModelSummary.sql"
        #self.file_TalkingPoint="..\Data\sql_TalkingPoints.sql"

    def createLeadsqlfilefromDB_1M(self,):
        #sql_Account_ID_Collection="select [Account_External_ID] from [dbo].[LeadCache] where [Account_External_ID] is not null group by [Account_External_ID]"
        #sql_Lead_ID_collection="select [Salesforce_ID] from [dbo].[LeadCache] where [Account_External_ID] is null group by [Salesforce_ID]"
        sql_LeadCache_info="select [External_ID], [Salesforce_ID], [Account_External_ID], [Recommendation_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date] from [dbo].[LeadCache]"
        file_name=self.file_LeadCache_1M
        print "===Start sql file for LeadCache==="
        try:
            conn=pyodbc.connect(self.conn_info)
            cursor=conn.cursor()
            print '===Start to query Leads info'
            Leads_Infos=cursor.execute(sql_LeadCache_info).fetchall()
            print str(len(Leads_Infos))
            print "queried results"
            f=open(file_name,'w')
            print "opened file"
            f.write("USE [DateBase_Name_TEMP]")
            f.write("\r\n")
            f.write("Declare @Custmer_ID nvarchar(50)"+"\n"
                         +"Declare @index_loop int \n"
                         +"set @Custmer_ID='[Defalut_Customer_ID]'"
                         +"\n"
                         +"set @index_loop=0"
                         +"\n"
                         +"if not exists (select * from sysobjects where id = object_id('[dbo].[LeadCache]'))"
                         +"\n"
                         +"begin\n"
                         +"print 'start create table ....'"
                         +"\n"
                         +"CREATE TABLE [dbo].[LeadCache]([LeadCache_ID] [int] IDENTITY(1,1) NOT NULL,\n"
                         +"	[External_ID] [nvarchar](50) NOT NULL,\n"
                         +"	[Salesforce_ID] [nvarchar](50) NULL,\n"
                         +"	[Account_External_ID] [nvarchar](50) NULL,\n"
                         +"	[Recommendation_ID] [int] NULL,\n"
                         +"	[Value] [nvarchar](max) NULL,\n"
                         +"	[Customer_ID] [nvarchar](50) NOT NULL,\n"
                         +"	[Creation_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"	[Last_Modification_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"PRIMARY KEY CLUSTERED\n"
                         +"(\n"
                         +"	[LeadCache_ID] ASC\n"
                         +")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n"
                         +") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\n"
                         +"end \n")
            f.write("\r\n")
            f.write("print 'start inserting ....'\n")
            f.write("while @index_loop<1000000 \n")
            f.write("print 'start inserting ....'\n")
            f.write("begin\n")
            for Lead_info in Leads_Infos:
                f.writelines("set @index_loop=@index_loop+1\n")
                f.writelines("if not exists (select * from [dbo].[LeadCache] where [External_ID]=@index_loop and [Customer_ID]= @Custmer_ID)\n"
                                 +"begin\n")
                f.write("INSERT [dbo].[LeadCache] ( [External_ID], [Salesforce_ID], [Account_External_ID], [Recommendation_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date]) VALUES ( @index_loop, ")
                if(Lead_info[1]==None):
                    f.write("NULL, ")
                else:
                    f.write("N'")
                    f.write(Lead_info[1].replace("'","''"))
                    f.write("', ")
                if(Lead_info[2]==None):
                    f.write("NULL, ")
                else:
                    f.write("N'")
                    f.write(Lead_info[2].replace("'","''"))
                    f.write("', ")
                f.write("@index_loop, ")
                if(Lead_info[4]==None):
                    f.write("NULL, ")
                else:
                    f.write("N'")
                    f.write(Lead_info[4].encode('utf-8').replace("'","''"))
                    f.write("', ")
                f.write("@Custmer_ID, CAST(N'"+str(Lead_info[6])[0:-3]+"' AS DateTime), CAST(N'"+str(Lead_info[7])[0:-3]+"' AS DateTime))")
                f.write("\n")
                f.write("end\n")
            f.write("end\n")
        except Exception , e:
            print "IO or sql connect issue"
            print e.message
        finally:
            cursor.close()
            conn.close()
            f.close()
            print "===Completed sql file for LeadCache==="

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--Server', dest = 'server', action = 'store', required = True, help = 'Server for DB')
    parser.add_argument('-D', '--DB_Name', dest = 'db_name', action = 'store', required = True, help = 'DB Name for Dnate')
    parser.add_argument('-U', '--username', dest = 'user', action = 'store', required = True, help = 'name of the DB user')
    parser.add_argument('-P', '--password', dest = 'pwd', action = 'store', required = True, help = 'password for DB user')
    args = parser.parse_args()

    Dante_test=CreateDanteSampleData(args.server,args.db_name,args.user,args.pwd)
    #Dante_test=CreateDanteSampleData('10.41.1.193\sql2008r2','DanteBackup','dataloader_user','password')
    Dante_test.createLeadsqlfilefromDB_1M()
