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
                                                    -nn [New Data Provider Name: if not specified, will not change] 
                                                    -dpt [Data Provider Type: sftp|sqlserver|sfdc|marketo|eloqua|
                                                        clarizen|oraclecrm, if not specified, will not change] 
                                                    -cs [ConnectionString: if not specified, will not change] 
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
                        "-cfp":  ("required", "[Config File Path] and [Structure File Path]"),
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



