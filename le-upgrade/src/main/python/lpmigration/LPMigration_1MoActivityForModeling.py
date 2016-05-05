
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import os, sys
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LPMigration_1MoActivityForModeling(StepBase):

    name        = 'LPMigration_1MoActivityForModeling'
    description = 'Adds 1-month activity aggregations appropriate for modeling to a Marketo LP2 tenant.'
    version     = '$Rev$'


    def __init__(self, forceApply=False):
        super(LPMigration_1MoActivityForModeling, self).__init__(forceApply)


    def getApplicability(self, appseq):
        if appseq.getText('template_type') == 'MKTO':
            return Applicability.canApply
        return Applicability.cannotApplyPass


    def apply( self, appseq ):
        print '\n    * Installing 1-month activity aggregations . .',
        sys.stdout.flush()
        newSpecsFileName = 'LP_MKTO_1MoModelingActivity.maude'
        newSpecsFileName = os.path.join('..','..','resources',newSpecsFileName)
        newSpecsFileName = os.path.join(os.path.dirname(__file__),newSpecsFileName)

        slnes = ''

        with open(newSpecsFileName, mode='r') as newSpecsFile:
            slnes = newSpecsFile.read()

        conn_mgr = appseq.getConnectionMgr()
        conn_mgr.setSpec('1-Month Activity for Modeling', slnes)
        
        return True
