
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020601_VDB_LeadRecord_Query(StepBase):

    name        = 'LP_020601_VDB_LeadRecord_Query'
    description = 'Install a query of all columns in MKTO_LeadRecord except LE_Domain'
    version     = '$Rev$'

    def __init__(self, forceApply = False):
        super(LP_020601_VDB_LeadRecord_Query, self).__init__(forceApply)

    def getApplicability(self, appseq):
        template_type = appseq.getText('template_type')
        if template_type =='MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass

    def apply(self, appseq):

        conn_mgr = appseq.getConnectionMgr()
        t_leadrecord = conn_mgr.getTable('MKTO_LeadRecord')
        q_leadrecord = QueryVDBImpl.initFromDefn('Q_MKTO_LeadRecord','SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionFromLAS(LatticeAddressSetMeet(empty)), LatticeAddressSetMeet(empty), LatticeAddressExpressionMeet((LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord")))))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName("MKTO_LeadRecord"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord")))) SpecQueryNamedFunctionEntityFunctionBoundary), SpecQueryResultSetAll)')
        for c in t_leadrecord.getColumnNames():
            if c in ['LE_Domain']:
                continue
            qc = QueryColumnVDBImpl(c, ExpressionVDBImplFactory.create('LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("{}")))'.format(c)))
            q_leadrecord.appendColumn(qc)
        conn_mgr.setQuery(q_leadrecord)

        return True
