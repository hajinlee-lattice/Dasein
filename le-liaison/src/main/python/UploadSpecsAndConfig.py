#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import sys
from lxml import etree
from liaison import *

def uploadSpecsAndConfig(tenantName, fileNameBase):
    
    specFileName = fileNameBase+'.specs'
    configFileName = fileNameBase+'.config'

    conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=tenantName)
    
    with open( specFileName ) as specFile:
        specstr = specFile.read()

    specstart_i = specstr.find('<specs>') + 7
    specend_i = specstr.find('</specs>')
    conn_mgr.setSpec('All Specs', specstr[specstart_i:specend_i])

    configxml = etree.parse(configFileName).getroot()
    configxml.set( 'appName', tenantName )

    conn_mgr.installDLConfigFile(etree.tostring(configxml,pretty_print=True))


def usage(cmd, exit_code):
    
    print ''
    print 'Usage: {0} <tenantName> <fileNameBase>'.format(cmd)
    print ''
    
    exit( exit_code )


if __name__ == "__main__":

    cmd = ''
    path = ''
    i = sys.argv[0].rfind('\\')
    if( i != -1 ):
        path = sys.argv[0][:i]
        cmd = sys.argv[0][i+1:]

    if len(sys.argv) == 1:
        usage( cmd, 0 )

    if len(sys.argv) != 3:
        usage( cmd, 1 )

    tenantName = sys.argv[1]
    fileNameBase = sys.argv[2]

    uploadSpecsAndConfig(tenantName, fileNameBase)
