'''
Created on May 7, 2014

@author: hliu
'''
from dbcreds import DBCreds

customers = []
for i in range(0, 75):
    customers.append("c"+str(i))
jetty_host = "bodcprodvutl158.prod.lattice.local"
yarn_host = "bodcprodvhdp196.prod.lattice.local"
table = "iris"
db_creds = DBCreds("bodcprodhdp210.prod.lattice.local", 3306, "dataplatformtest", "root", "welcome")
training_percentage = 80
key_cols = "ID"
features = "SEPAL_LENGTH,SEPAL_WIDTH,PETAL_LENGTH,PETAL_WIDTH"
target = "CATEGORY"