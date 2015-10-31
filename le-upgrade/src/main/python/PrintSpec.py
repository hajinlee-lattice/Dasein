
import appsequence,liaison

tenant = 'Tmpl_LP_Trunk_ELQ_Main'
#tenant = 'Tmpl_LP_02-00-01_ELQ_Main'

conn_mgr = liaison.ConnectionMgrFactory.Create( 'visiDB', tenant_name=tenant, verify=False )

q = conn_mgr.getQuery('Q_Dante_LeadSourceTable')

print ''

i = 0
for f in q.getFilters():
    print 'Filter {0}: {1}'.format( i, f.Definition() )
    print ''
    i += 1

j = 0
for e in q.getEntities():
    print 'Entity {0}: {1}'.format( j, e.Definition() )
    print ''
    j += 1



exit(0)


## first example
q = conn_mgr.GetQuery('Q_ELQ_Contact_Score')
filters_new = []
filters_new.append('LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName("ELQ_Contact_ContactID_IsSelectedForPushToDestination"))')
filters_new.append('LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName("Timestamp_PushToDante")), LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("Timestamp_PushToDante"))))')
q.setFilters( filters_new )
