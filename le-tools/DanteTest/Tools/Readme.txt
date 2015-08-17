********************************
There are 2 scripts:


Script1: CreateDanteSampleDataSqlFile
--------------------------------------------
 run this script, we can get 4 sql files from one dante DB we configured in instance variables. These sql files stored schema and data in Tables.The sql file will check the target table, if there are record for the account or lead, the sample data will not be inserted:
    [FrontEndCombinedModelSummaryCache]
    [LeadCache]
    [AccountCache]
    [TalkingPointCache]

  And the default Path to save them is .\Data folder, if you want to change the save path, please modify them in __init__ method before you run the script.
  
  
  Note: Please check the .\Data folder exist if you use the default save path.

  Following are the parameters required when we run the script:
    -S the server name for target DB (e.g. 10.41.1.87\SQL2012STD)
    -D The source DanteDB name
    -U User name to log in the DB server
    -P password to log in DB server

  The example to run the script is :

  step1: cd to the folder stored the script (cd <rootfolder>\PreparSampleDanteData)
  setp2: run the command ( python CreateDanteSampleDataSqlFile.py -S 10.41.1.87\SQL2012STD -D DT_ADEDTBDd720133nQ280105n154 -U dataloader_user -P password)




 -------------------------------------------

 Script 2: ExportDanteSampleData
 -------------------------------------------
 Run this script, we can populate the sample data stored in sql file into the target Dante DB, In this version we need to populate data one table by one table.
 Following are the parameters required:
    -S the server name for target DB (e.g. 10.41.1.87\SQL2012STD)
    -U User name to log in the DB server
    -P password to log in DB server
    -i the sql file path we want to populate the data in them
    -rd The target DanteDB name
    -rc The customer ID for target Dante DB

  The example to run the script is :

  step1: cd to the folder stored the script (cd <rootfolder>\PreparSampleDanteData)
  setp2: run the command to export sampe Data for one table:
 ( python CreateDanteSampleDataSqlFile.py -S 10.41.1.87\SQL2012STD -U dataloader_user -P password -i .\Data\sql_AccountCache1.sql -rd DT_ABADEDTANwiaadd720133nC28010 -rc ABADEDTANwiaadd720133nC28010)
  step3: change the input file path (-i ****) to export other table.