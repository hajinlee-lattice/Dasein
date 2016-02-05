
#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase
import appsequence
import liaison

class LP_020300_VDB_ModifiedColumns( StepBase ):
  
  name        = 'LP_020100_VDB_ModifiedColumns'
  description = 'Upgrade Modified Specs from 2.0.1 to 2.1.0'
  version     = '$Rev: 70934 $'
  def __init__( self, forceApply = False ):
    super( LP_020300_VDB_ModifiedColumns, self ).__init__( forceApply )

  def getApplicability( self, appseq ):

      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      if type == 'SFDC':
          conn_mgr.getTable("PD_DerivedColumns")
          if not conn_mgr.getTable("PD_DerivedColumns") :
              return Applicability.cannotApplyFail
      else:
          return Applicability.cannotApplyPass
      return  Applicability.canApply

  def apply( self, appseq ):

      success = False
      conn_mgr = appseq.getConnectionMgr()
      type = appseq.getText( 'template_type' )
      if type == 'SFDC':
      # Modify the Column in Timestamp_PushToDante
       table1 = conn_mgr.getTable( 'PD_DerivedColumns')
       col1_1 = liaison.TableColumnVDBImpl( 'BusinessIndustry', 'PD_DerivedColumns', 'NVarChar(500)')
       table1.appendColumn( col1_1)
       conn_mgr.setTable( table1 )
      else:
          pass

      success = True

      return success






