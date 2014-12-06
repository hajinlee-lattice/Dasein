#!/usr/local/bin/python
# coding: utf-8

# Environment configuration config for envInstaller.py
# !!! should not be used without it !!!

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules (if any)
import os

# All decoratable functions (if any) should be here. Before config.


# Eclipse will complain that config is 'Undefined variable'
# and many other things. IGNORE IT!
# execfile(config_name, globals()) will take care of it all
# Do not add 'config = OrderedDict()'
# It will override everything else already in envInstaller!

PATH_TO_DEPOT = "/home/ivinnichenko/develop/ledp"
LE_DP = "/home/ivinnichenko/develop/ledp/le-dataplatform"
SQL_FILES_LOCATION = PATH_TO_DEPOT + "/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/mysql"
USERNAME = "ivinnichenko"

# Configure SVN
config["svn"] = {1:("cmd_from_dir", ["mvn -DskipTests clean install", PATH_TO_DEPOT]),
                 2:("cmd_from_dir", ["mvn -DskipTests clean package dependency:copy-dependencies", PATH_TO_DEPOT]),
                 3:("cmd_from_dir",
                    ["java -cp %s/target/*:%s/target/dependency/* com.latticeengines.dataplatform.dao.impl.SchemaGenerator" % (LE_DP, LE_DP),
                     LE_DP])
                 }

# Configure MySQL
config["mysql"] = {1:("cmd", "mysql -uroot --password=welcome < %s/create_ledp.sql" % SQL_FILES_LOCATION),
                   2:("@py", [update_file,
                              "%s/create_LeadScoringDB.sql" % SQL_FILES_LOCATION,
                              "LOAD DATA INFILE '%s/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_train.dat'\n" % PATH_TO_DEPOT,
                              0.9, "max_last_equal"]),
                   3:("@py", [update_file,
                              "%s/create_LeadScoringDB.sql" % SQL_FILES_LOCATION,
                              "LOAD DATA INFILE '%s/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_test.dat'\n" % PATH_TO_DEPOT,
                              0.9, "max_last_equal"]),
                   4:("cmd", "mysql -uroot --password=welcome < %s/create_LeadScoringDB.sql" % SQL_FILES_LOCATION),
                   5:("@py", [add_file_header, "%s/le-dataplatform/ddl_dlorchestration_mysql.sql" % PATH_TO_DEPOT, "USE LeadScoringDB;\n/*"]),
                   6:("@py", [update_file, "%s/le-dataplatform/ddl_dlorchestration_mysql.sql" % PATH_TO_DEPOT,
                              "*/    drop table if exists `LeadScoringCommandLog`;\n", 0.8]),
                   7:("cmd", "mysql -uroot --password=welcome < %s/le-dataplatform/ddl_dlorchestration_mysql.sql" % PATH_TO_DEPOT),
                   8:("@py", [add_file_header, "%s/le-dataplatform/ddl_ledp_mysql.sql" % PATH_TO_DEPOT, "USE ledp;\n/*"]),
                   9:("@py", [update_file, "%s/le-dataplatform/ddl_ledp_mysql.sql" % PATH_TO_DEPOT,
                              "*/    drop table if exists `ALGORITHM`;\n", 0.8]),
                   10:("cmd", "mysql -uroot --password=welcome < %s/le-dataplatform/ddl_ledp_mysql.sql" % PATH_TO_DEPOT)
                   }

# Configure Hadoop
config["hadoop"] = {1:("cmd_from_dir", ["mvn -DskipTests clean dependency:copy-dependencies deploy", PATH_TO_DEPOT]),
                    2:("cmd", "cp %s/le-scheduler/target/le-scheduler-1.0.8-SNAPSHOT.jar $HADOOP_HOME/share/hadoop/yarn" % PATH_TO_DEPOT),
                    3:("cmd", "cp %s/le-scheduler/conf/env/dev/fair-scheduler.xml $HADOOP_HOME/etc/hadoop" % PATH_TO_DEPOT),
                    4:("@py", [update_file, "%s/etc/hadoop/fair-scheduler.xml" % os.getenv("HADOOP_HOME"),
                                '    <user name="%s">\n' % USERNAME, 0.4])
                    }

#config["test"] =  dict([(i, config["svn"][i]) for i in [3] if i in config["svn"]])