
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020601_DL_LeadRecordReimport(StepBase):

    name        = 'LP_020601_DL_LeadRecordReimport'
    description = 'Install a load group to run an extract query from MKTO_LeadRecord back into MKTO_LeadRecord'
    version     = '$Rev$'

    def __init__(self, forceApply = False):
        super(LP_020601_DL_LeadRecordReimport, self).__init__(forceApply)

    def getApplicability(self, appseq):
        template_type = appseq.getText('template_type')
        if template_type =='MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass

    def apply(self, appseq):

        conn_mgr = appseq.getConnectionMgr()
        lg_mgr = appseq.getLoadGroupMgr()

        q_leadrecord = conn_mgr.getQuery('Q_MKTO_LeadRecord')

        lg_mgr.createLoadGroup('Adhoc_MKTO_LeadRecord_Reimport', 'Adhoc', 'Adhoc_MKTO_LeadRecord_Reimport', True, False)
        eqxml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_MKTO_LeadRecord" queryAlias="Q_MKTO_LeadRecord" sw="Workspace" schemaName="MKTO_LeadRecord" at="False" ucm="True"><schemas/><specs/><cms>'
        for c in q_leadrecord.getColumnNames():
            if c not in ['MKTO_LeadRecord','EntityFunctionBoundary']:
                eqxml += '<cm qcn="{0}" itcn="{0}"/>'.format(c)
        eqxml += '</cms></extractQuery></extractQueries>'
        lg_mgr.setLoadGroupFunctionality('Adhoc_MKTO_LeadRecord_Reimport', eqxml)

        lg_mgr.commit()

        return True
