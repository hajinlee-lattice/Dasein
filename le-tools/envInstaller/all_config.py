#!/usr/local/bin/python
# coding: utf-8

# Main installation config for envInstaller.py
# Installs everything at once. May be not the best option...
# !!! should not be used without envInstaller.py !!!

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

# Install Python libraries
config["python"] = {1: ("cmd", "sudo apt-get install python-setuptools -y"),
                    2: ("cmd", "sudo apt-get install python-pip python-dev build-essential -y"),
                    3: ("cmd", "sudo pip install numpy"),
                    4: ("cmd", "sudo apt-get install libatlas-base-dev gfortran -y"),
                    5: ("cmd", "sudo pip install scipy"),
                    6: ("cmd", "sudo pip install scikit-learn"),
                    7: ("cmd", "sudo pip install pandas"),
                    8: ("cmd", "sudo pip install fastavro"),
                    9: ("cmd", "sudo pip install coverage"),
                    10:("cmd", "sudo pip install avro"),
                    11:("@py", [update_file, "~/.bashrc",
                                "export PYTHON_HOME=/usr/bin/python\n", 0.7]),
                    12:("@py", [update_file, "~/.bashrc", "export PATH=$PYTHON_HOME/bin:$PATH\n", 1.0])
                    }

# Install Java
config["java"] = {1:("cmd", "sudo apt-get -y install openjdk-7-jdk"),
                  2:("@py", [update_file, "~/.bashrc",
                             "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64\n", 0.8]),
                  3:("@py", [update_file, "~/.bashrc",
                             "export JAVA_LIB=/usr/lib/x86_64-linux-gnu/jni\n", 0.8]),
                  4:("@py", [update_file, "~/.bashrc", "export PATH=$JAVA_HOME/bin:$PATH\n", 1.0]),
                  5:("@py", [update_file, "~/.bashrc", "export PATH=$JAVA_LIB/bin:$PATH\n", 1.0]),
                  }


# Install Eclipse (TARs should be downloaded from (\\le-file\Groups\Engineering Setup) manually,
# untill IT learns how to setup fixed name Linux network folder)
config["eclipse"] = {1:("cmd", "sudo tar -zvxf %s.tar.gz" % ECLIPSE),
                     2:("cmd", "sudo cp -R eclipse /opt"),
                     3:("cmd", "sudo chown -R root:root /opt/eclipse"),
                     4:("cmd", "sudo ln -s /opt/eclipse/eclipse /usr/bin/eclipse"),
                     # The templates should be downloaded with TARs
                     5:("cmd", "sudo cp ./eclipse.desktop.tmpl /usr/share/applications/eclipse.desktop")
                     }

# Install Maven
config["maven"] = {1:("cmd", "sudo apt-get install maven -y")}

# Install SVN
config["svn"] = {1:("cmd", "sudo apt-get install subversion -y")}

# Install Review Board
config["rbtools"] = {1:("cmd", "sudo apt-get install python-setuptools -y"),
                     2:("cmd", "sudo easy_install -U RBTools"),
                     3:("@py", [update_file, "~/.reviewboardrc",
                                "REVIEWBOARD_URL='http://bodcdevvrvw65.lattice.local/rb'\n", 0.7]),
                     4:("cmd", "sudo sudo chmod 777 ~/.reviewboardrc")
                     }

# Install monitoring
config["monitoring"] = {1:("cmd", "sudo apt-get install ganglia-monitor -y"),
                        # ganglia-webfrontend does not obbey -y for some reason...
                        2:("cmd", "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y ganglia-webfrontend")
                        }

# Install SSHD
config["sshd"] = {1:("cmd", "sudo apt-get install openssh-server -y"),
                  2:("cmd", "rm -f ~/.ssh/id_dsa"),
                  3:("cmd", "ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa"),
                  4:("cmd", "cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys"),
                  5:("cmd", "sudo chmod 777 ~/.ssh/authorized_keys")
                  }

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
