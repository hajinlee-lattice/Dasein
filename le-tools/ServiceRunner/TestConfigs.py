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

ConfigDLC["Launch"] = {"command": "-L",
                       "definition": "dlc -L -s [DataLoader URL] -u [User Name] -p [Password] -li [Launch Id]",
                       "-s": ("required", "DataLoader URL"),
                       "-u": ("required", "User Name"),
                       "-p": ("required", "Password"),
                       "-li": ("required", "Launch Id")
                       }

