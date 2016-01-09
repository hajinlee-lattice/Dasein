
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from liaison import *
from appsequence import Applicability, AppSequence, StepBase
from lxml import etree

class LP_020201_DL_PushToLeadDestination_Validation( StepBase ):

    name        = 'LP_020201_DL_PushToLeadDestination_Validation'
    description = 'Remove PushToLeadDestination_Validation from PushToLeadDestination'
    version     = '$Rev$'


    def __init__( self, forceApply = False ):
        super( LP_020201_DL_PushToLeadDestination_Validation, self ).__init__( forceApply )


    def getApplicability( self, appseq ):
        
        lg_mgr = appseq.getLoadGroupMgr()

        ptld_ngs_xml = lg_mgr.getLoadGroupFunctionality('PushToLeadDestination', 'ngs')
        ptld_ngs = etree.fromstring( ptld_ngs_xml )
        for ng in ptld_ngs:
            if ng.get('n') == 'PushToLeadDestination_Validation':
                return Applicability.canApply

        return Applicability.cannotApplyPass


    def apply( self, appseq ):

        lg_mgr = appseq.getLoadGroupMgr()

        ptld_ngs_xml = lg_mgr.getLoadGroupFunctionality('PushToLeadDestination', 'ngs')
        ptld_ngs = etree.fromstring( ptld_ngs_xml )
        for ng in ptld_ngs:
            if ng.get('n') == 'PushToLeadDestination_Validation':
                ptld_ngs.remove( ng )
        lg_mgr.setLoadGroupFunctionality('PushToLeadDestination', etree.tostring(ptld_ngs))
        
        return True
