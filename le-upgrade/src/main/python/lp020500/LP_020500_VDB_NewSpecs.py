
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-12-17 02:20:10 +0800 (Thu, 17 Dec 2015) $
# $Rev: 71693 $
#

import os
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020500_VDB_NewSpecs( StepBase ):

  name        = 'LP_020100_VDB_NewSpecs'
  description = 'Adds new specs to the visiDB template'
  version     = '$Rev: 71693 $'


  def __init__( self, forceApply = False ):
    super( LP_020500_VDB_NewSpecs, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    return Applicability.canApply


  def apply( self, appseq ):
    template_type = appseq.getText( 'template_type' )

    newSpecsFileName = 'LP_' + template_type + '_NewSpecs_2.5.0_from_2.4.3.maude'
    newSpecsFileName = os.path.join('..','..','resources',newSpecsFileName)
    newSpecsFileName = os.path.join(os.path.dirname(__file__),newSpecsFileName)

    slnes = ''

    with open( newSpecsFileName, mode='r' ) as newSpecsFile:
      slnes = newSpecsFile.read()

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.setSpec( 'New Specs', slnes )
    
    return True
