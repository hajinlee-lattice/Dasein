
#
# $LastChangedBy: VivianZhao $
# $LastChangedDate: 2015-10-29 14:33:11 +0800 (Wed, 21 Oct 2015) $
# $Rev: 70508 $
#

from lxml import etree
from appsequence import Applicability, StepBase

class LP_020100_DL_LoadCRMData( StepBase ):
  
  name        = 'LP_020100_DL_LoadCRMData.py'
  description = 'Reset logic of the LoadCRMData'
  version     = '$Rev: 70508 $'

  def __init__( self, forceApply = False ):
    super( LP_020100_DL_LoadCRMData, self ).__init__( forceApply )


  def getApplicability( self, appseq ):

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )
    if type == 'MKTO' or type == 'ELQ':
      if lgm.hasLoadGroup( 'LoadCRMData' ) and lgm.hasLoadGroup( 'LoadCRMDataForModeling' ):
        return Applicability.canApply
    elif type == 'SFDC':
      return Applicability.cannotApplyPass
    return Applicability.cannotApplyFail


  def apply( self, appseq ):
    
    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText( 'template_type' )

    lgm.getLoadGroup( 'LoadCRMData')
    step1xml = ''
    if type == 'MKTO':
      step1xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Leads" queryAlias="Q_Timestamp_DownloadedSFDC_Leads" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Leads" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Leads" itcn="Time_OfComepletion_DownloadedSFDC_Leads" /></cms></extractQuery><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Contacts" queryAlias="Q_Timestamp_DownloadedSFDC_Contacts" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Contacts" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Contacts" itcn="Time_OfComepletion_DownloadedSFDC_Contacts" /></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step1xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Leads" queryAlias="Q_Timestamp_DownloadedSFDC_Leads" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Leads" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Leads" itcn="Time_OfComepletion_DownloadedSFDC_Leads" /></cms></extractQuery><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Contacts" queryAlias="Q_Timestamp_DownloadedSFDC_Contacts" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Contacts" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Contacts" itcn="Time_OfComepletion_DownloadedSFDC_Contacts" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'LoadCRMData', step1xml )

    lgm.getLoadGroup( 'LoadCRMDataForModeling')

    step3xml = ''
    if type == 'MKTO':
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Leads" queryAlias="Q_Timestamp_DownloadedSFDC_Leads" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Leads" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Leads" itcn="Time_OfComepletion_DownloadedSFDC_Leads" /></cms></extractQuery><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Contacts" queryAlias="Q_Timestamp_DownloadedSFDC_Contacts" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Contacts" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Contacts" itcn="Time_OfComepletion_DownloadedSFDC_Contacts" /></cms></extractQuery></extractQueries>'
    elif type =='ELQ':
      step3xml = '<extractQueries><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Leads" queryAlias="Q_Timestamp_DownloadedSFDC_Leads" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Leads" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Lead_ID" itcn="SFDC_Lead_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Leads" itcn="Time_OfComepletion_DownloadedSFDC_Leads" /></cms></extractQuery><extractQuery qw="Workspace" queryName="Q_Timestamp_DownloadedSFDC_Contacts" queryAlias="Q_Timestamp_DownloadedSFDC_Contacts" sw="Workspace" schemaName="Timestamp_DownloadedSFDC_Contacts" at="False" ucm="True"><schemas /><specs /><cms><cm qcn="SFDC_Contact_ID" itcn="SFDC_Contact_ID" /><cm qcn="Time_OfComepletion_DownloadedSFDC_Contacts" itcn="Time_OfComepletion_DownloadedSFDC_Contacts" /></cms></extractQuery></extractQueries>'
    lgm.setLoadGroupFunctionality( 'LoadCRMDataForModeling', step3xml )
    lgm.commit()

    success = True

    return success

