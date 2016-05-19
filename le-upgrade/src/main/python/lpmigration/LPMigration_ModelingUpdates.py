
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_ModelingUpdates(StepBase):

    name        = 'LPMigration_ModelingUpdates'
    description = 'Updates various specs to improve modeling'
    version     = '$Rev$'


    def __init__(self, forceApply=False):
        super(LPMigration_ModelingUpdates, self).__init__(forceApply)


    def getApplicability(self, appseq):
        return Applicability.canApply


    def apply( self, appseq ):
        print '\n    * Installing modeling updates . .',
        sys.stdout.flush()

        ## Install a new verions of "Lead_InDateRangeForModeling"
        conn_mgr = appseq.getConnectionMgr()
        spec_Lead_InDateRangeForModeling = 'SpecLatticeFunction(LatticeFunctionExpressionTransform(LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AND"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Greater"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("AddDay"), LatticeFunctionIdentifier(ContainerElementName("Alias_ModifiedDate_Lead")), LatticeFunctionIdentifier(ContainerElementName("Const_DaysOfDataForModeling"))), LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementName("Alias_ModifiedDate_Lead")), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)), FunctionAggregationOperator("None"))), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Greater"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Minus"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("ConvertToInt"), LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementName("Alias_CreatedDate_Lead")), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)), FunctionAggregationOperator("None"))), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("IsNull"), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("ConvertToInt"), LatticeFunctionIdentifier(ContainerElementName("Alias_CreatedDate_Lead"))), LatticeFunctionExpressionConstantScalar("0", DataTypeInt))), LatticeFunctionExpression(LatticeFunctionOperatorIdentifier("Multiply"), LatticeFunctionExpressionConstant("86400", DataTypeInt), LatticeFunctionIdentifier(ContainerElementName("Const_DaysFromLeadCreationDate"))))), LatticeAddressSetIdentifier(ContainerElementName("Alias_AllLeadTable")), FunctionAggregationOperator("Max")), DataTypeUnknown, SpecFunctionTypeMetric, SpecFunctionSourceTypeCalculation, SpecDefaultValueNull, SpecDescription(""))'
        conn_mgr.setNamedExpression(NamedExpressionVDBImpl.InitFromDefn('Lead_InDateRangeForModeling', spec_Lead_InDateRangeForModeling))

        ## Update "Const_DaysFromLeadCreationDate"
        exp_Const_DaysFromLeadCreationDate = conn_mgr.getNamedExpression('Const_DaysFromLeadCreationDate')
        exp_Const_DaysFromLeadCreationDate.Object().SetValue(90)
        conn_mgr.setNamedExpression(exp_Const_DaysFromLeadCreationDate)

        return True
