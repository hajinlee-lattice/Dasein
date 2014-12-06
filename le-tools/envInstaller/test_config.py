#!/usr/local/bin/python
# coding: utf-8

# Test config for envInstaller.py should not be used without it

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

# Decoratable function for testing basic config functionality
# "test" mode only
def simple_test(p):
    print(p)

# Decoratable function for testing Exceptions handling
# "test" mode only
def test_exception():
    print "This function should raise ValueError exception"
    raise ValueError

# Eclipse will complain that config is 'Undefined variable'
# and many other things. IGNORE IT!
# execfile(config_name, globals()) will take care of it all
# Do not add 'config = OrderedDict()'
# It will override everything else already in envInstaller!

config["test"] = {1:("cmd", "ls -lah"),
                  2:("@py", [simple_test,"Python Rocks!!!"]),
                  4:("@py", simple_test),
                  3:("test", "ku-ku"),
                  5:("@py", [test_exception])
                }