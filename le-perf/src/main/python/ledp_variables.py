'''
Created on May 7, 2014

@author: hliu
'''
from dbcreds import DBCreds

customers = []
for i in range(0, 75):
    customers.append("c" + str(i))
jetty_host = "bodcprodvutl158.prod.lattice.local"
yarn_host = "bodcprodvhdp196.prod.lattice.local"
table = "Q_EventTableDepivot"
db_creds = DBCreds("10.41.1.250", 1433, "dataplatformtest", "root", "welcome")
metadata_table = "EventMetadata"
training_percentage = 80
key_cols = "Nutanix_EventTable_Clean"
target = "P1_Event_1"
