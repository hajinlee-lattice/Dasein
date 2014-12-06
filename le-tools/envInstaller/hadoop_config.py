#!/usr/local/bin/python
# coding: utf-8
#from envInstaller import SQOOP

# Hadoop installation config for envInstaller.py
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

# All decoratable functions (if any) should be here. Before config.


# Eclipse will complain that config is 'Undefined variable'
# and many other things. IGNORE IT!
# execfile(config_name, globals()) will take care of it all
# Do not add 'config = OrderedDict()'
# It will override everything else already in envInstaller!

# Install Hadoop (TARs should be downloaded from (\\le-file\Groups\Engineering Setup) manually,
# untill IT learns how to setup fixed name Linux network folder)
config["hadoop"] = {1: ("cmd", "tar -zvxf %s.tar.gz" % HADOOP),
                    2: ("@py", [update_file, "~/.bashrc",
                               "export HADOOP_HOME=$HOME/Tools/%s\n" % HADOOP, 1.0]),
                    3: ("@py", [update_file, "~/.bashrc",
                               "export HADOOP_MAPRED_HOME=$HOME/Tools/%s\n" % HADOOP, 1.0]),
                    4: ("@py", [update_file, "~/.bashrc",
                               "export HADOOP_COMMON_HOME=$HOME/Tools/%s\n" % HADOOP, 1.0]),
                    5: ("@py", [update_file, "~/.bashrc",
                               "export HADOOP_HDFS_HOME=$HOME/Tools/%s\n" % HADOOP, 1.0]),
                    6: ("@py", [update_file, "~/.bashrc",
                               "export HADOOP_YARN_HOME=$HOME/Tools/%s\n" % HADOOP, 1.0]),
                    7: ("@py", [update_file, "~/.bashrc",
                               "export HADOOP_CONF_DIR=$HOME/Tools/%s/etc/hadoop\n" % HADOOP, 1.0]),
                    8: ("@py", [update_file, "~/.bashrc", "export YARN_HOME=$HADOOP_HOME\n", 1.0]),
                    9: ("@py", [update_file, "~/.bashrc", "export PATH=$PATH:$HADOOP_HOME/bin\n", 1.0]),
                    10:("@py", [update_file, "~/.bashrc", "export PATH=$PATH:$HADOOP_HOME/sbin\n", 1.0]),
                    11:("@py", [update_file, "~/.bashrc", "export M2_REPO=~/.m2/repository\n", 1.0]),
                    12:("cmd", "mkdir -p %s/yarn/yarn_data/hdfs/namenode" % YARN_HOME),
                    13:("cmd", "chmod 777 %s/yarn/yarn_data/hdfs/namenode" % YARN_HOME),
                    14:("cmd", "mkdir -p %s/yarn/yarn_data/hdfs/datanode" % YARN_HOME),
                    15:("cmd", "chmod 777 %s/yarn/yarn_data/hdfs/datanode" % YARN_HOME),
                    # The templates should be in the same directory as installation script and TARs
                    16:("cmd", "cp yarn-site.xml.tmpl %s/etc/hadoop/yarn-site.xml" % YARN_HOME),
                    17:("cmd", "cp core-site.xml.tmpl %s/etc/hadoop/core-site.xml" % YARN_HOME),
                    18:("cmd", "cp hdfs-site.xml.tmpl %s/etc/hadoop/hdfs-site.xml" % YARN_HOME),
                    19: ("@py", [update_file,
                                 "%s/etc/hadoop/hdfs-site.xml" % YARN_HOME,
                                 "<value>file:%s/yarn/yarn_data/hdfs/namenode</value>\n" % YARN_HOME,
                                 0.9, "max_last_equal"]),
                    20: ("@py", [update_file,
                                 "%s/etc/hadoop/hdfs-site.xml" % YARN_HOME,
                                 "<value>file:%s/yarn/yarn_data/hdfs/datanode</value>\n" % YARN_HOME,
                                 0.9, "max_last_equal"]),
                    21:("cmd", "cp mapred-site.xml.tmpl %s/etc/hadoop/mapred-site.xml" % YARN_HOME),
                    22: ("@py", [update_file,
                                 "%s/etc/hadoop/hadoop-env.sh" % YARN_HOME,
                                 "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64", 0.3]),
                    23:("cmd_from_dir", ["hadoop namenode -format", YARN_HOME])
                    }

# Install SQOOP (TARs should be downloaded from (\\le-file\Groups\Engineering Setup) manually,
# untill IT learns how to setup fixed name Linux network folder)
config["sqoop"] = {1:("cmd", "sudo tar -zvxf %s.tar.gz" % SQOOP),
                   2:("@py", [update_file, "~/.bashrc", "export SQOOP_HOME=$HOME/Tools/%s\n" % SQOOP, 1.0]),
                   3:("@py", [update_file, "~/.bashrc", "export PATH=$PATH:$SQOOP_HOME/bin\n", 1.0])
                   }
