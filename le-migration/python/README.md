## Requirements

The python part of this project is written in Python3, and to run this project, the following dependencies are required:

openssl 1.0.2r (for compatibility with SQLAlchemy)<br/>
SQLAlchemy 1.3.5<br/>
mysqlclient 1.3.14<br/>

Please install them in your python environment.

## Scripts

Please add the following lines to `~/.bash_profile` and source it:
```
alias trackTenant="python $WSHOME/le-migration/python/scripts/trackTenant.py"
alias updateMigrationStatus="python $WSHOME/le-migration/python/scripts/updateMigrationStatus.py"
```

To add a tenant to the migration table:<br/>
(The default database name is PLS_MultiTenant)
```
trackTenant -u <db username> -p <db password> -x <db host> [-d <db name>] -t TENANT_PID
```

To update the migration status of a tenant to the migration table:<br/>
(The migration status is case insensitive)
```
updateMigrationStatus -u <username> -p <password> -x <host> [-d <db name>] -t TENANT_PID -s <SCHEDULED/STARTED/FAILED/COMPLETED>
```

## DB Storage Engine Usage (Optional)<br/>
You can provide the following environment variables:<br/>
MYSQL_USER --> MySQL server user<br/>
MYSQL_PWD  --> MySQL password<br/>
MYSQL_HOST --> MySQL server host<br/>
MYSQL_DB   --> MySQL DB. It should be `PLS_MultiTenant`, but you can use another db for testing purpose<br/>
Then in you python script, you can do:
```
from models import storage
trashTenant = storage.all('Tenant')[5]
deletedTenant = trashTenant.delete()
print(deletedTenant)
...
```

If you wish to create storage engine on multiple database with the same set of defined models, DO NOT set any environment variables. In your python script:
```
from models.engine.db_storage import DBStorage
s1 = DBStorage(MYSQL_USER='username', MYSQL_PWD='pwd', MYSQL_HOST='host', MYSQL_DB='dbname') #pass kwargs
s2 = DBStorage('username', 'pwd', 'another host', 'dbname') # or pass args in this order
...
```
