#!/usr/local/bin/python
# coding: utf-8

# Languages installation config for envInstaller.py
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
