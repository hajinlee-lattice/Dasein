#!/usr/local/bin/python
# coding: utf-8

# Base test framework configs

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules

from collections import OrderedDict


ConfigCSV = OrderedDict()

#"DataScience_StandardAttributes.csv":"DataScience_StandardAttributes",
ConfigCSV["Marketo"] = {"Cfg_ColumnsForQA.csv":"Cfg_ColumnsForQA",
                        "Cfg_LinksToMostRecentSamples.csv":"Cfg_LinksToMostRecentSamples",
                        "Cfg_PLS_Event.csv":"Cfg_PLS_Event",
                        "Cfg_Custom_Attributes.csv":"Cfg_Custom_Attributes",
                        "Cfg_PLS_EventTableQueries.csv":"Cfg_PLS_EventTableQueries"}

ConfigCSV["Eloqua"] = {"Cfg_ColumnsForQA_ELQ.csv":"Cfg_ColumnsForQA",
                       "Cfg_LinksToMostRecentSamples_ELQ.csv":"Cfg_LinksToMostRecentSamples",
                       "Cfg_PLS_Event.csv":"Cfg_PLS_Event",
                       "Cfg_Custom_Attributes.csv":"Cfg_Custom_Attributes",
                       "Cfg_PLS_EventTableQueries.csv":"Cfg_PLS_EventTableQueries"}

ConfigCSV["Salesforce"] = {"Cfg_ColumnsForQA.csv":"Cfg_ColumnsForQA",
                       "Cfg_LinksToMostRecentSamples.csv":"Cfg_LinksToMostRecentSamples",
                       "Cfg_PLS_Event.csv":"Cfg_PLS_Event",
                       "Cfg_Custom_Attributes.csv":"Cfg_Custom_Attributes",
                       "Cfg_PLS_EventTableQueries.csv":"Cfg_PLS_EventTableQueries"}

EtlConfig = OrderedDict()

EtlConfig["Eloqua"] = {"CrmPassword": "Happy2010",
                        "CrmSecurityToken": "uMWcnIq9rCOxtRteKKgixE26",
                        "CrmSystem": "Salesforce",
                        "CrmUserName": "apeters-widgettech@lattice-engines.com",
                        "DeploymentExternalID": "", # Tenant
                        "DataLoaderTenantName": "", # Tenant
                        "ReportsDBDataSource": "",
                        "ReportsDBInitialCatalog": "",
                        "ScoringDBDataSource": "",
                        "ScoringDBInitialCatalog": "",
                        "DanteDBDataSource": "",
                        "DanteDBInitialCatalog": "",
                        # Make sure you have protocol in your URL (you'll get an error otherwise)
                        "DataLoaderURL": "https://bodcdevvint187.dev.lattice.local:8080",
                        # Marketting App specific
                        "Topology": "EloquaAndSalesforceToEloqua",
                        "LeadFlowSource": "Eloqua",
                        "LeadFlowTarget": "Eloqua",
                        "LeadFlowTargetField": "C_Lattice_Lead_Score1",
                        "MapCompany": "TechnologyPartnerLatticeEngines",
                        "MapPassword": "Lattice1",
                        "MapSecurityToken": "None",
                        "MapSystem": "Eloqua",
                        "MapURL": "None",
                        "MapUserName": "Matt.Sable"
                       }

EtlConfig["Marketo"] = {"CrmPassword": "Happy2010",
                        "CrmSecurityToken": "uMWcnIq9rCOxtRteKKgixE26",
                        "CrmSystem": "Salesforce",
                        "CrmUserName": "apeters-widgettech@lattice-engines.com",
                        "DeploymentExternalID": "", # Tenant
                        "DataLoaderTenantName": "", # Tenant
                        "ReportsDBDataSource": "",
                        "ReportsDBInitialCatalog": "",
                        "ScoringDBDataSource": "",
                        "ScoringDBInitialCatalog": "",
                        "DanteDBDataSource": "",
                        "DanteDBInitialCatalog": "",
                        # Make sure you have protocol in your URL (you'll get an error otherwise)
                        "DataLoaderURL": "https://bodcdevvint187.dev.lattice.local:8080",
                        # Marketting App specific
                        "Topology": "MarketoAndSalesforceToMarketo",
                        "LeadFlowSource": "Marketo",
                        "LeadFlowTarget": "Marketo",
                        "LeadFlowTargetField": "LeadScore",
                        "MapCompany": "None",
                        "MapPassword": "None",
                        "MapSecurityToken": "41802295835604145500BBDD0011770133777863CA58",
                        "MapSystem": "Marketo",
                        "MapURL": "https://na-sj02.marketo.com/soap/mktows/2_0",
                        "MapUserName": "latticeenginessandbox1_9026948050BD016F376AE6"
                       }


ConfigDLC = OrderedDict()

ConfigDLC["Test Command"] = {"command": "-Test",
                       "definition": "dlc -Test -s [DataLoader URL] -u [User Name] -p [Password] -li [Launch Id]",
                       "-s": ("required", "DataLoader URL"),
                       "-u": ("required", "User Name"),
                       "-p": ("optional", "Password"),
                       "-li": ("unknown", "Launch Id")
                       }

ConfigDLC["Launch"] = {"command": "-L",
                       "definition": "dlc -L -s [DataLoader URL] -u [User Name] -p [Password] -li [Launch Id]",
                       "-s":  ("required", "DataLoader URL"),
                       "-u":  ("required", "User Name"),
                       "-p":  ("required", "Password"),
                       "-li": ("required", "Launch Id")
                       }

ConfigDLC["New Data Provider"] = {"command": "-NDP",
                                  "definition": """dlc -NDP -s [DataLoader URL] -u [User Name] -p [Password] 
                                                    -t [Tenant Name] -dpn [Data Provider Name] -cs [Connection String]
                                                    -dpf [Connection String Used For: upload|validation extract|
                                                    leaf extract|itc|fstable, split with '|'] 
                                                    -dpt [Data Provider Type:sftp|sqlserver|sfdc|marketo|eloqua|
                                                    clarizen|oraclecrm|eloqua bulk] -v [Verify Connection: true|false,
                                                    default value is false] -guid [GUID: the guid for log]""",
                                  "-s":    ("required", "DataLoader URL"),
                                  "-u":    ("required", "User Name"),
                                  "-p":    ("required", "Password"),
                                  "-t":    ("required", "Tenant Name"),
                                  "-dpn":  ("required", "Data Provider Name"),
                                  "-dpf":  ("required", "Connection String"),
                                  "-cs":   ("required", "Connection String"),
                                  "-dpt":  ("required", "Data Provider Type"),
                                  "-v":    ("optional", "Verify Connection"),
                                  "-guid": ("optional", "GUID: the guid for log")
                                  }

ConfigDLC["Edit Data Provider"] = {"command": "-EDP",
                                   "definition": """dlc -EDP -s [DataLoader URL] -u [User Name] -p [Password] 
                                                    -t [Tenant Name] -dpn [DataProviderName] 
                                                    -nn [New Data Provider Name: ] 
                                                    -dpt [Data Provider Type: sftp|sqlserver|sfdc|marketo|eloqua|
                                                        clarizen|oraclecrm, ] 
                                                    -cs [ConnectionString: ] 
                                                    -dpf [Connection String Used For: upload|validation extract|
                                                        leaf extract|itc|fstable, split with '|', if not specified, 
                                                        will not change] 
                                                    -v [Verify Connection: true|false, default value is false] 
                                                    -guid [GUID: the guid for log, this argument is not must have]""",
                                    "-s":    ("required", "DataLoader URL"),
                                    "-u":    ("required", "User Name"),
                                    "-p":    ("required", "Password"),
                                    "-t":    ("required", "Tenant Name"),
                                    "-dpn":  ("required", "Data Provider Name"),
                                    "-nn":   ("optional", "New Data Provider Name"),
                                    "-dpt":  ("optional", "Data Provider Type"),
                                    "-cs":   ("optional", "Connection String"),
                                    "-dpf":  ("optional", "Connection String"),
                                    "-v":    ("optional", "Verify Connection"),
                                    "-guid": ("optional", "GUID: the guid for log")
                                    }

ConfigDLC["Migrate"] = {"command": "-M",
                        "definition": """dlc -M -s [DataLoader URL] -u [User Name] -p [Password] -t [Tenant Name] 
                                         -cfp [Config File Path] -sfp [Structure File Path] 
                                         -mm [Migrate Mode: append|replace] -r [Reason: it can be empty] 
                                         -kj [Kill End User Job: true|false, default value is true] 
                                         -sync [Synchronous: true|false, default value is true] 
                                         -guid [GUID: the guid for log]""",
                        "-s":    ("required", "DataLoader URL"),
                        "-u":    ("required", "User Name"),
                        "-p":    ("required", "Password"),
                        "-t":    ("required", "Tenant Name"),
                        "-cfp":  ("required", "[Config File Path]"),
                        "-sfp":  ("required", "[Structure File Path]"),
                        "-mm":   ("required", "Migrate Mode"),
                        "-r":    ("optional", "Reason"),
                        "-kj":   ("optional", "Kill End User Job"),
                        "-sync": ("optional", "Synchronous"),
                        "-guid": ("optional", "GUID: the guid for log")
                        }

ConfigDLC["Launch Load Group"] = {"command": "-LG",
                                  "definition": """dlc -LG -s [DataLoader URL] -u [User Name] -p [Password] 
                                                   -t [Tenant Name] -g [Group Name] 
                                                   -sync [Synchronous: true|false, default value is false.]
                                                   -guid [GUID: the guid for log]""",
                                  "-s":    ("required", "DataLoader URL"),
                                  "-u":    ("required", "User Name"),
                                  "-p":    ("required", "Password"),
                                  "-t":    ("required", "Tenant Name"),
                                  "-g":    ("required", "Group Name"),
                                  "-sync": ("optional", "Synchronous"),
                                  "-guid": ("optional", "GUID: the guid for log")
                                  }

ConfigDLC["Get Load Group Status"] = {"command": "-GLS",
                                      "definition": """dlc -GLS -s [DataLoader URL] -u [User Name] -p [Password] 
                                                       -t [Tenant Name] -g [GroupName] -guid [the guid for log]""",
                                      "-s":    ("required", "DataLoader URL"),
                                      "-u":    ("required", "User Name"),
                                      "-p":    ("required", "Password"),
                                      "-t":    ("required", "Tenant Name"),
                                      "-g":    ("required", "Group Name"),
                                      "-guid": ("optional", "GUID: the guid for log")
                                     }


ConfigDLC["New Load Group"] = {"command": "-NLG",
                               "definition": """dlc -NLG -s [DataLoader URL] -u [User Name] -p [Password] 
                                                -t [Tenant Name] -g [Group Name] -a [Alias: is group name]
                                                -gp [Path: default value is empty] -thr [Threshold: default is 10000]
                                                -gt [Load Group Type: manual|autopopulated|manualandautopopulated] 
                                                -st [Execution Rule: immediate|afterhours|scheduled, default immediate] 
                                                -acst [Allow End User Change Execution Rule: true|false, default false] 
                                                -v [Visible For End User: true|false, default value is true] 
                                                -cof [Auto Clear On Failure: true|false, default value is false] 
                                                -led [Launch Expired Days: default value is 7] 
                                                -vm [Validation Valid Minutes: default value is from tenant settings] 
                                                -dg [Dependent Group: default value is empty] 
                                                -cg [Copy From Groups: the names of groups are separated by ','] 
                                                -frt [First Run Time: yyyy-MM-dd HH:mm:ss TZD] 
                                                -rut [Repeat Until Time: yyyy-MM-dd HH:mm:ss TZD] 
                                                -rf [Repeat Frequency:] 
                                                -wd [Days:]
                                                -guid [GUID: the guid for log, this argument is not must have]""",
                                      "-s":    ("required", "DataLoader URL"),
                                      "-u":    ("required", "User Name"),
                                      "-p":    ("required", "Password"),
                                      "-t":    ("required", "Tenant Name"),
                                      "-g":    ("required", "Group Name"),
                                      "-a":    ("optional", "Alias"),
                                      "-gp":   ("optional", "Path"),
                                      "-thr":  ("optional", "Threshold"),
                                      "-gt":   ("optional", "Load Group Type"),
                                      "-st":   ("optional", "Execution Rule"),
                                      "-acst": ("optional", "Allow End User Change Execution Rule"),
                                      "-v":    ("optional", "Visible For End User"),
                                      "-cof":  ("optional", "Auto Clear On Failure"),
                                      "-led":  ("optional", "Launch Expired Days"),
                                      "-vm":   ("optional", "Validation Valid Minutes"),
                                      "-dg":   ("optional", "Dependent Group"),
                                      "-cg":   ("optional", "Copy From Group"),
                                      "-frt":  ("optional", "First Run Time"),
                                      "-rut":  ("optional", "Repeat Until Time"),
                                      "-rf":   ("optional", "Repeat Frequency"),
                                      "-wd":   ("optional", "Days"),
                                      "-guid": ("optional", "GUID: the guid for log")
                                     }

ConfigDLC["New Refresh Data Source"] = {"command": "-NRDS",
                                        "definition": """dlc -NRDS -s [DataLoader URL] -u [User Name] -p [Password] 
                                                        -t [Tenant Name] -g [Group Name] -rn [Refresh Data Source Name] 
                                                        -sn [Table Schema Name]  -cn [Data Provider Name: ] 
                                                        -usqls [Use SQL Statement: true|false, default value is false] 
                                                        -sqls [SQL Statement: default value is empty] 
                                                        -tab [Table Name: default value is empty] 
                                                        -f [Filters: default value is empty] 
                                                        -ade [Delete Expired Extracts Automatically: true|false] 
                                                        -em [Exact Match: true|false, default value is true] 
                                                        -rcs [Required Columns: the names of columns, separated by '|'] 
                                                        -td [Throttling Data: true|false, default value is false] 
                                                        -idc [Id Column] -diff [Date Diff] 
                                                        -limit [Limitation: default value is 1000] 
                                                        -stw [Split into Time Windows: true|false] 
                                                        -csr [Confidence Succeed Rows: default value is 50000] 
                                                        -htw [Hours of Time Window: default value is 24] 
                                                        -mmtw [Minutes of Min Time Window: default value is 60] 
                                                        -mdrc [Max Days of Record Count: default value is 365] 
                                                        -ewmt [extract Data With Multiple Threads: default is false] 
                                                        -acd [Always Commit Data] 
                                                        -guid [GUID: the guid for log]""",
                                      "-s":    ("required", "DataLoader URL"),
                                      "-u":    ("required", "User Name"),
                                      "-p":    ("required", "Password"),
                                      "-t":    ("required", "Tenant Name"),
                                      "-g":    ("required", "Group Name"),
                                      "-rn":   ("required", "Refresh Data Source Name"),
                                      "-sn":   ("required", "Table Schema Name"),
                                      "-cn":   ("required", "Data Provider Name"),
                                      "-usqls":("optional", "Use SQL Statement"),
                                      "-sqls": ("optional", "SQL Statement"),
                                      "-tab":  ("optional", "Table Name"),
                                      "-f":    ("optional", "Filters"),
                                      "-ade":  ("optional", "Delete Expired Extracts Automatically"),
                                      "-em":   ("optional", "Exact Match"),
                                      "-rcs":  ("optional", "Required Columns"),
                                      "-td":   ("optional", "Throttling Data"),
                                      "-idc":  ("optional", "Id Column"),
                                      "-limit":("optional", "Limitation"),
                                      "-stw":  ("optional", "Split into Time Windows"),
                                      "-csr":  ("optional", "Confidence Succeed Rows"),
                                      "-htw":  ("optional", "Hours of Time Window"),
                                      "-mmtw": ("optional", "Minutes of Min Time Window"),
                                      "-mdrc": ("optional", "Max Days of Record Count"),
                                      "-ewmt": ("optional", "extract Data With Multiple Threads"),
                                      "-acd ": ("optional", "Always Commit Data"),
                                      "-guid": ("optional", "GUID: the guid for log")
                                     }

ConfigDLC["Edit Refresh Data Source"] = {"command": "-ERDS",
                                         "definition": """dlc -ERDS -s [DataLoader URL] -u [User Name] 
                                                         -p [Password] -t [Tenant Name] -g [Group Name]
                                                         -rn [Refresh Data Source Name] -sn [Table Schema Name: ]
                                                         -cn [Data Provider Name: ] -usqls [Use SQL Statement: ]
                                                         -sqls [SQL Statement: ] -tab [Table Name: ] -f [Filters: ]
                                                         -ade [Delete Expired Extracts Automatically: true|false] 
                                                         -em [Exact Match: true|false, ]
                                                         -rcs [Required Columns: the names of columns are separated by '|']
                                                         -td [Throttling Data: ] -idc [Id Column: ]
                                                         -diff [Date Diff: ] -limit [Limitation: ]
                                                         -stw [Split into Time Windows: ] -csr [Confidence Succeed Rows: ]
                                                         -htw [Hours of Time Window: ]
                                                         -mmtw [Minutes of Min Time Window: ]
                                                         -mdrc [Max Days of Record Count: ]
                                                         -ewmt [extract Data With Multiple Threads: ]
                                                         -acd [Always Commit Data: ] -guid [GUID: the guid for log]""",
                                         "-s":    ("required", "DataLoader URL"),
                                         "-u":    ("required", "User Name"),
                                         "-p":    ("required", "Password"),
                                         "-t":    ("required", "Tenant Name"),
                                         "-g":    ("required", "Group Name"),
                                         "-rn":   ("required", "Refresh Data Source Name"),
                                         "-sn":   ("optional", "Table Schema Name"),
                                         "-cn":   ("optional", "Data Provider Name"),
                                         "-usqls":("optional", "Use SQL Statement"),
                                         "-sqls": ("optional", "SQL Statement"),
                                         "-tab":  ("optional", "Table Name"),
                                         "-f":    ("optional", "Filters"),
                                         "-ade":  ("optional", "Delete Expired Extracts Automatically"),
                                         "-em":   ("optional", "Exact Match"),
                                         "-rcs":  ("optional", "Required Columns"),
                                         "-td":   ("optional", "Throttling Data"),
                                         "-idc":  ("optional", "Id Column"),
                                         "-diff": ("optional", "Date Diff"),
                                         "-limit":("optional", "Limitation"),
                                         "-stw":  ("optional", "Split into Time Windows"),
                                         "-csr":  ("optional", "Confidence Succeed Rows"),
                                         "-htw":  ("optional", "Hours of Time Window"),
                                         "-mmtw": ("optional", "Minutes of Min Time Window"),
                                         "-mdrc": ("optional", "Max Days of Record Count"),
                                         "-ewmt": ("optional", "extract Data With Multiple Threads"),
                                         "-acd ": ("optional", "Always Commit Data"),
                                         "-guid": ("optional", "GUID: the guid for log")
                                         }

ConfigDLC["DeTach Visidb DataBase"] = {"command": "-RD",
                                         "definition": """dlc -RD -s [DataLoader URL] -u [User Name] 
                                                         -p [Password] -dc [VisiDB Connection String] 
                                                         -dn [VisiDB Name] 
                                                         -guid [GUID: the guid for log]""",
                                         "-s":    ("required", "DataLoader URL"),
                                         "-u":    ("required", "User Name"),
                                         "-p":    ("required", "Password"),
                                         "-dc":    ("required", "VisiDB Connection String"),
                                         "-dn":    ("required", "VisiDB Name"),
                                         "-guid": ("optional", "GUID: the guid for log")
                                         }
ConfigDLC["Delete Visidb DataBase"] = {"command": "-DD",
                                         "definition": """dlc -DD -s [DataLoader URL] -u [User Name] 
                                                         -p [Password] -dc [VisiDB Connection String] 
                                                         -dn [VisiDB Name] 
                                                         -guid [GUID: the guid for log]""",
                                         "-s":    ("required", "DataLoader URL"),
                                         "-u":    ("required", "User Name"),
                                         "-p":    ("required", "Password"),
                                         "-dc":    ("required", "VisiDB Connection String"),
                                         "-dn":    ("required", "VisiDB Name"),
                                         "-guid": ("optional", "GUID: the guid for log")
                                         }
ConfigDLC["Attach Visidb DataBase"] = {"command": "-AD",
                                         "definition": """dlc -AD -s [DataLoader URL] -u [User Name] 
                                                         -p [Password] -dc [VisiDB Connection String] 
                                                         -dn [VisiDB Name]
                                                         -cl [size of the DB]
                                                         -dfd [db definition file] 
                                                         -guid [GUID: the guid for log]""",
                                         "-s":    ("required", "DataLoader URL"),
                                         "-u":    ("required", "User Name"),
                                         "-p":    ("required", "Password"),
                                         "-dc":    ("required", "VisiDB Connection String"),
                                         "-dn":    ("required", "VisiDB Name"),
                                         "-cl":    ("required", "size of the DB"),
                                         "-dfd":    ("required", "db definition file"),
                                         "-guid": ("optional", "GUID: the guid for log")
                                         }

