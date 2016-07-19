
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020601_VDB_LeadRecord_LEDomain(StepBase):

    name        = 'LP_020601_VDB_LeadRecord_LEDomain'
    description = 'Ensure that LE_Domain in MKTO_LeadRecord is a TableFunction'
    version     = '$Rev$'

    def __init__(self, forceApply = False):
        super(LP_020601_VDB_LeadRecord_LEDomain, self).__init__(forceApply)

    def getApplicability(self, appseq):
        template_type = appseq.getText('template_type')
        if template_type =='MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass

    def apply(self, appseq):

        conn_mgr = appseq.getConnectionMgr()
        ledomain_tf_spec = 'SpecColumnBinding(ContainerElementName("LE_Domain"), DataTypeVarChar(150), TableFunctionExpression(TableFunctionOperator("SubString"), TableFunctionList(TableFunctionColumn(ContainerElementName("Email")) TableFunctionExpression(TableFunctionOperator("Plus"), TableFunctionList(TableFunctionExpression(TableFunctionOperator("LeftCharIndex"), TableFunctionList(TableFunctionColumn(ContainerElementName("Email")) TableFunctionConstant(ScalarValueTypedString("@", VectorDataTypeString(4))))) TableFunctionConstant(ScalarValueTypedString("1", VectorDataTypeWord)))) TableFunctionExpression(TableFunctionOperator("Length"), TableFunctionList(TableFunctionColumn(ContainerElementName("Email")))))))'
        ledomain_bug_spec = 'SpecColumnBinding(ContainerElementName("LE_Domain"),DataTypeVarChar(150))'

        t_leadrecord = conn_mgr.getTable('MKTO_LeadRecord')
        #for c in t_leadrecord.getColumnNames():
        #    print c
        slne = t_leadrecord.SpecLatticeNamedElements()
        slne = slne.replace(ledomain_bug_spec, ledomain_tf_spec)
        conn_mgr.setSpec("MKTO_LeadRecord", slne)

        return True
