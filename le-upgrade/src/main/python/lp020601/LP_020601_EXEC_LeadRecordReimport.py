
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020601_EXEC_LeadRecordReimport(StepBase):

    name        = 'LP_020601_EXEC_LeadRecordReimport'
    description = 'EXECUTE the load group to run an extract query from MKTO_LeadRecord back into MKTO_LeadRecord'
    version     = '$Rev$'

    def __init__(self, forceApply = False):
        super(LP_020601_EXEC_LeadRecordReimport, self).__init__(forceApply)

    def getApplicability(self, appseq):
        template_type = appseq.getText('template_type')
        if template_type =='MKTO':
            return Applicability.canApply

        return Applicability.cannotApplyPass

    def apply(self, appseq):

        conn_mgr = appseq.getConnectionMgr()

        launchid = conn_mgr.executeGroup('Adhoc_MKTO_LeadRecord_Reimport', 'mwilson@lattice-engines.com')

        return True
