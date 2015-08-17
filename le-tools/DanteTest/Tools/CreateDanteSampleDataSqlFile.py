'''
@author: nxu
'''

import pyodbc
import os
import argparse

class CreateDanteSampleData(object):
    
    def __init__(self,server='10.41.1.87\SQL2012STD',DataBase='DT_ADEDTBDd720133nQ280105n154',User='dataloader_user',Password='password'):
        self.conn_info="DRIVER={SQL Server};Server="+server+";DATABASE="+DataBase+";UID="+User+";PWD="+Password
        self.file_LeadCache="..\Data\sql_LeadCache.sql"
        self.file_AccountCache="..\Data\sql_AccountCache.sql"
        self.file_FECModelSummary="..\Data\sql_FECModelSummary.sql"
        self.file_TalkingPoint="..\Data\sql_TalkingPoints.sql"

    def createTalkingPointfromDB(self,):
        sql_TalkingPoint_info="select [External_ID], [Play_External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date] from [dbo].[TalkingPointCache]"
        sql_Play_ID_collection="select [Play_External_ID] from [dbo].[TalkingPointCache] where [Play_External_ID] is not null group by [Play_External_ID]"
        file_name=self.file_TalkingPoint
        print "===Start sql file for TalkingPointCache==="
        try:
            conn=pyodbc.connect(self.conn_info)
            cursor=conn.cursor()
            All_Play_IDs=cursor.execute(sql_Play_ID_collection).fetchall()
            #print "queried results"
            f=open(file_name,'w')
            #print "opened file"
            f.write("USE [DateBase_Name_TEMP]")
            f.write("\r\n")
            f.write("Declare @Custmer_ID nvarchar(50)"+"\r\n"
                         +"set @Custmer_ID='[Defalut_Customer_ID]'"
                         +"\r\n"
                         +"if not exists (select * from sysobjects where id = object_id('[dbo].[TalkingPointCache]'))"
                         +"\n"
                         +"begin\n"
                         +"CREATE TABLE [dbo].[TalkingPointCache](\n"
                         +"	[TalkingPointCache_ID] [int] IDENTITY(1,1) NOT NULL,\n"
                         +"	[External_ID] [nvarchar](50) NOT NULL,\n"
                         +"	[Play_External_ID] [nvarchar](50) NOT NULL,\n"
                         +"[Value] [nvarchar](max) NULL,\n"
                         +"[Customer_ID] [nvarchar](50) NOT NULL,\n"
                         +"[Creation_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"[Last_Modification_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"PRIMARY KEY CLUSTERED\n"
                         +"(\n"
                         +"[TalkingPointCache_ID] ASC\n"
                         +")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n"
                         +") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\n"
                         +"end \n")
            f.write("\r\n")
            for Play_ID in All_Play_IDs:
                Talk_Types_Play=cursor.execute(sql_TalkingPoint_info+" where [Play_External_ID]='"+Play_ID[0].replace("'","''")+"'").fetchall()
                f.write("if not exists (select * from [dbo].[TalkingPointCache] where [Play_External_ID]='")
                f.write(Play_ID[0].replace("'","''"))
                f.write("') \n begin \n")
                for Talk_Type in Talk_Types_Play:
                    f.write(" INSERT [dbo].[TalkingPointCache] ([External_ID], [Play_External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date]) VALUES (N'")
                    f.write(Talk_Type[0].replace("'","''"))
                    f.write("',N'")
                    f.write(Talk_Type[1].replace("'","''"))
                    f.write("',N'")
                    f.write(Talk_Type[2].encode('utf-8').replace("'","''") )
                    f.write("',@Custmer_ID,CAST(N'"+str(Talk_Type[4])[0:-3]+"' AS DateTime),CAST(N'"+str(Talk_Type[5])[0:-3]+"' AS DateTime))")
                    f.write("\n")
                f.write("end\n")
        except Exception , e:
            print "IO or sql connect issue"
            print e.message
        finally:
            cursor.close()
            conn.close()
            f.close()
            print "===Complete sql file for TalkingPointCache==="

    def createFrontEndModelfromDB(self,):
        sql_FNCModelSumary_info="select [External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date] from [dbo].[FrontEndCombinedModelSummaryCache]"
        file_name=self.file_FECModelSummary
        print "===Start sql file for FrontEndCombinedModelSummaryCache==="
        try:
            conn=pyodbc.connect(self.conn_info)
            cursor=conn.cursor()
            All_FNCModelSumary=cursor.execute(sql_FNCModelSumary_info).fetchall()
            cursor.close()
            #print "queried results"
            f=open(file_name,'w')
            #print "opened file"
            f.write("USE [DateBase_Name_TEMP]")
            f.write("\r\n")
            f.write("Declare @Custmer_ID nvarchar(50)"+"\r\n"
                         +"set @Custmer_ID='[Defalut_Customer_ID]'"
                         +"\r\n"
                         +"if not exists (select * from sysobjects where id = object_id('[dbo].[FrontEndCombinedModelSummaryCache]'))"
                         +"\n"
                         +"begin\n"
                         +"CREATE TABLE [dbo].[FrontEndCombinedModelSummaryCache](\n"
                         +"[FrontEndCombinedModelSummaryCache_ID] [int] IDENTITY(1,1) NOT NULL,\n"
                         +"[External_ID] [nvarchar](50) NOT NULL,\n"
                         +"[Value] [nvarchar](max) NULL,\n"
                         +"[Customer_ID] [nvarchar](50) NOT NULL,\n"
                         +"[Creation_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"[Last_Modification_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"PRIMARY KEY CLUSTERED\n"
                         +"(\n"
                         +"[FrontEndCombinedModelSummaryCache_ID] ASC\n"
                         +")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n"
                         +") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\n"
                         +"end \n")
            f.write("\r\n")
            for FNCModelSumary in All_FNCModelSumary:
                f.write("if not exists (select * from [dbo].[FrontEndCombinedModelSummaryCache] where [External_ID]='")
                f.write(FNCModelSumary[0].replace("'","''"))
                f.write("') INSERT [dbo].[FrontEndCombinedModelSummaryCache] ([External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date]) VALUES (N'")
                f.write(FNCModelSumary[0].replace("'","''"))
                f.write("',N'")
                f.write(FNCModelSumary[1].encode('utf-8').replace("'","''") )
                f.write("',@Custmer_ID,CAST(N'"+str(FNCModelSumary[3])[0:-3]+"' AS DateTime),CAST(N'"+str(FNCModelSumary[4])[0:-3]+"' AS DateTime))")
                f.write("\n")
        except Exception , e:
            print "IO or sql connect issue"
            print e.message
        finally:
            conn.close()
            f.close()
            print "===Completed sql file for FrontEndCombinedModelSummaryCache==="

    def createAccountsqlfilefromDB(self,):
        sql_AccountCache_info="select [External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date] from [dbo].[AccountCache]"
        file_name=self.file_AccountCache
        print "===Start sql file for AccountCache==="
        try:
            conn=pyodbc.connect(self.conn_info)
            cursor=conn.cursor()
            All_AccountCache=cursor.execute(sql_AccountCache_info).fetchall()
            cursor.close()
            #print "queried results"
            f=open(file_name,'w')
            #print "opened file"
            f.write("USE [DateBase_Name_TEMP]")
            f.write("\r\n")
            f.write("Declare @Custmer_ID nvarchar(50)"+"\r\n"
                         +"set @Custmer_ID='[Defalut_Customer_ID]'"
                         +"\r\n"
                         +"if not exists (select * from sysobjects where id = object_id('[dbo].[AccountCache]'))"
                         +"\n"
                         +"begin\n"
                         +"CREATE TABLE [dbo].[AccountCache]([AccountCache_ID] [int] IDENTITY(1,1) NOT NULL,\n"
                         +"[External_ID] [nvarchar](50) NOT NULL,\n"
                         +"[Value] [nvarchar](max) NULL,\n"
                         +"[Customer_ID] [nvarchar](50) NOT NULL,\n"
                         +"[Creation_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"[Last_Modification_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"PRIMARY KEY CLUSTERED\n"
                         +"(\n"
                         +"[AccountCache_ID] ASC\n"
                         +")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n"
                         +") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\n"
                         +"end \n")
            f.write("\r\n")
            for account in All_AccountCache:
                f.write("if not exists (select * from [dbo].[AccountCache] where [External_ID]='")
                f.write(account[0].replace("'","''"))
                f.write("') INSERT [dbo].[AccountCache] ([External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date]) VALUES (N'")
                f.write(account[0].replace("'","''"))
                f.write("',N'")
                f.write(account[1].encode('utf-8').replace("'","''") )
                f.write("',@Custmer_ID,CAST(N'"+str(account[3])[0:-3]+"' AS DateTime),CAST(N'"+str(account[4])[0:-3]+"' AS DateTime))")
                f.write("\n")
        except Exception , e:
            print "IO or sql connect issue"
            print e.message
        finally:
            conn.close()
            f.close()
            print "===Completed sql file for AccountCache==="

    def createLeadsqlfilefromDB(self,):
        sql_Account_ID_Collection="select [Account_External_ID] from [dbo].[LeadCache] where [Account_External_ID] is not null group by [Account_External_ID]"
        sql_Lead_ID_collection="select [Salesforce_ID] from [dbo].[LeadCache] where [Account_External_ID] is null group by [Salesforce_ID]"
        sql_LeadCache_info="select [External_ID], [Salesforce_ID], [Account_External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date] from [dbo].[LeadCache]"
        file_name=self.file_LeadCache
        print "===Start sql file for LeadCache==="
        try:
            conn=pyodbc.connect(self.conn_info)
            cursor=conn.cursor()
            Account_IDs=cursor.execute(sql_Account_ID_Collection).fetchall()
            Leads_IDs=cursor.execute(sql_Lead_ID_collection).fetchall()
            #print "queried results"
            f=open(file_name,'w')
            #print "opened file"
            f.write("USE [DateBase_Name_TEMP]")
            f.write("\r\n")
            f.write("Declare @Custmer_ID nvarchar(50)"+"\r\n"
                         +"set @Custmer_ID='[Defalut_Customer_ID]'"
                         +"\r\n"
                         +"if not exists (select * from sysobjects where id = object_id('[dbo].[LeadCache]'))"
                         +"\n"
                         +"begin\n"
                         +"print 'start create table ....'"
                         +"\n"
                         +"CREATE TABLE [dbo].[LeadCache]([LeadCache_ID] [int] IDENTITY(1,1) NOT NULL,\n"
                         +"	[External_ID] [nvarchar](50) NOT NULL,\n"
                         +"	[Salesforce_ID] [nvarchar](50) NULL,\n"
                         +"	[Account_External_ID] [nvarchar](50) NULL,\n"
                         +"	[Value] [nvarchar](max) NULL,\n"
                         +"	[Customer_ID] [nvarchar](50) NOT NULL,\n"
                         +"	[Creation_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"[Last_Modification_Date] [datetime] NOT NULL DEFAULT (getdate()),\n"
                         +"PRIMARY KEY CLUSTERED\n"
                         +"(\n"
                         +"	[LeadCache_ID] ASC\n"
                         +")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n"
                         +") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\n"
                         +"end \n")
            f.write("\r\n")
            f.write("print 'start inserting ....'\n")
            for account in Account_IDs:
                Play_models_account=cursor.execute(sql_LeadCache_info+" where [Account_External_ID]='"+account[0].replace("'","''")+"'").fetchall()
                f.writelines("if not exists (select * from [dbo].[LeadCache] where [Account_External_ID]='"+account[0].replace("'","''")+"')\n"
                             +"begin\n")
                for play in Play_models_account:
                    f.write("INSERT [dbo].[LeadCache] ( [External_ID], [Salesforce_ID], [Account_External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date]) VALUES ( N'")
                    f.write(play[0].replace("'","''"))
                    f.write("', NULL, N'")
                    f.write(play[2].replace("'","''"))
                    f.write("', N'")
                    f.write(play[3].encode('utf-8').replace("'","''"))
                    f.write("',@Custmer_ID, CAST(N'"+str(play[5])[0:-3]+"' AS DateTime), CAST(N'"+str(play[6])[0:-3]+"' AS DateTime))")
                    f.write("\n")
                f.write("end\n")
            for leads in Leads_IDs:
                Score_models_Leads=cursor.execute(sql_LeadCache_info+" where [Salesforce_ID]='"+leads[0].replace("'","''")+"'").fetchall()
                f.write("if not exists (select * from [dbo].[LeadCache] where [Salesforce_ID]='"+leads[0].replace("'","''")+"')\n"
                             +"begin\n")
                for model_score in Score_models_Leads:
                    s_encode_value=model_score[3].encode('utf-8')
                    f.write("INSERT [dbo].[LeadCache] ( [External_ID], [Salesforce_ID], [Account_External_ID], [Value], [Customer_ID], [Creation_Date], [Last_Modification_Date]) VALUES ( N'")
                    f.write(model_score[0].replace("'","''"))
                    f.write("', N'")
                    f.write(model_score[1].replace("'","''"))
                    f.write("', NULL, N'")
                    f.write(s_encode_value.replace("'","''"))
                    f.write("',@Custmer_ID, CAST(N'"+ str(model_score[5])[0:-3]+"' AS DateTime), CAST(N'"+str(model_score[6])[0:-3]+"' AS DateTime))")
                    f.write("\n")
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
    Dante_test.createAccountsqlfilefromDB()
    Dante_test.createFrontEndModelfromDB()
    Dante_test.createLeadsqlfilefromDB()
    Dante_test.createTalkingPointfromDB()
