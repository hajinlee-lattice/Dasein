
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_1MoActivityForScoring(StepBase):

    name        = 'LPMigration_1MoActivityForScoring'
    description = 'Adds 1-month activity aggregations appropriate for scoring to a Marketo LP2 tenant.'
    version     = '$Rev$'


    def __init__(self, forceApply=False):
        super(LPMigration_1MoActivityForScoring, self).__init__(forceApply)


    def getApplicability(self, appseq):
        if appseq.getText('template_type') == 'MKTO':
            return Applicability.canApply
        return Applicability.cannotApplyPass


    def apply( self, appseq ):
        print '\n    * Installing 1-month activity aggregations for scoring . .',
        sys.stdout.flush()
        newSpecsFileName = 'LP_MKTO_1MoScoringActivity.maude'
        newSpecsFileName = os.path.join('..','..','resources',newSpecsFileName)
        newSpecsFileName = os.path.join(os.path.dirname(__file__),newSpecsFileName)

        slnes = ''

        with open(newSpecsFileName, mode='r') as newSpecsFile:
            slnes = newSpecsFile.read()

        conn_mgr = appseq.getConnectionMgr()
        conn_mgr.setSpec('1-Month Activity for Scoring', slnes)

        return True
