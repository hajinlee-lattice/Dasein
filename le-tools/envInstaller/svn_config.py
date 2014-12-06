#!/usr/local/bin/python
# coding: utf-8

# SVN installation config for envInstaller.py
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

config["mysql"] = {1:("cmd", "sudo ./install_mysql.sh"),
                   2:("cmd", "sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/usr.sbin.mysqld"),
                   3:("cmd", "sudo /etc/init.d/apparmor reload")
                   }