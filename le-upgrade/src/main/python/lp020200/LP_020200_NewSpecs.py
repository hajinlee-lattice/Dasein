
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-10-31 08:34:49 +0800 (Sat, 31 Oct 2015) $
# $Rev: 70711 $
#

import os
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020200_NewSpecs( StepBase ):

  name        = 'LP_020200_NewSpecs'
  description = 'Adds new specs to the visiDB template'
  version     = '$Rev: 70711 $'


  def __init__( self, forceApply = False ):
    super( LP_020200_NewSpecs, self ).__init__( forceApply )


  def getApplicability( self, appseq ):
    return Applicability.canApply


  def apply( self, appseq ):
    template_type = appseq.getText( 'template_type' )

    newSpecsFileName = 'LP_' + template_type + '_NewSpecs_2.2.0_from_2.1.2.maude'
    newSpecsFileName = os.path.join('..','resources',newSpecsFileName)

    slnes = ''

    with open( newSpecsFileName, mode='r' ) as newSpecsFile:
      slnes = newSpecsFile.read()

    conn_mgr = appseq.getConnectionMgr()
    conn_mgr.setSpec( 'New Specs', slnes )
    
    return True
