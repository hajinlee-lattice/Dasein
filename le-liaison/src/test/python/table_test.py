
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-10-29 22:59:04 -0700 (Thu, 29 Oct 2015) $
# $Rev: 70693 $
#

import sys, os
sys.path.append( os.path.join(os.path.dirname(__file__),'..','..','main','python') )

import liaison

tenant = 'Tmpl_LP_Trunk_ELQ_Main'
conn_mgr = liaison.ConnectionMgrFactory.Create( 'visiDB', tenant_name=tenant, verify=False )

t = conn_mgr.getTable('ELQ_Contact')
print ''
for c in t.getColumns():
    print c.getName()
    print c.definition()
    print ''
