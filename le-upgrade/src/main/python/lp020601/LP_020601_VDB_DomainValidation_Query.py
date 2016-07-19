
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020601_VDB_DomainValidation_Query(StepBase):

    name        = 'LP_020601_VDB_DomainValidation_Query'
    description = 'Install a query for validating the population of LE_Domain'
    version     = '$Rev$'

    def __init__(self, forceApply = False):
        super(LP_020601_VDB_DomainValidation_Query, self).__init__(forceApply)

    def getApplicability(self, appseq):
        template_type = appseq.getText('template_type')
        if template_type =='MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass

    def apply(self, appseq):

        conn_mgr = appseq.getConnectionMgr()
        q_validatedomain = QueryVDBImpl.initFromDefn('Q_MKTO_LeadRecord_ValidateLEDomain','SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionFromLAS(LatticeAddressSetMeet(empty)), LatticeAddressSetMeet(empty), LatticeAddressExpressionMeet((LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord")))))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName("MKTO_LeadRecord"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord")))) SpecQueryNamedFunctionEntityFunctionBoundary SpecQueryNamedFunctionExpression(ContainerElementName("Email"), LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("Email"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("Email")))), FunctionAggregationOperator("Max"))) SpecQueryNamedFunctionExpression(ContainerElementName("LE_Domain"), LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("LE_Domain"))), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("LE_Domain")))), FunctionAggregationOperator("Max")))), SpecQueryResultSetAll)')
        q_validatedomain.setFilters([QueryFilterVDBImpl('LatticeAddressSetFcn(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("NOT"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IsNullValue"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName("MKTO_LeadRecord")), ContainerElementName("LE_Domain"))))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName("MKTO_LeadRecord_ID")))))')])
        conn_mgr.setQuery(q_validatedomain)

        return True
