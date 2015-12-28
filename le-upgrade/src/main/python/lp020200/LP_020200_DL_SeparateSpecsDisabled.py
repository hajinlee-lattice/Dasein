#
# $LastChangedBy: mwilson $
# $LastChangedDate: 2015-11-13 14:56:48 +0800 (Fri, 13 Nov 2015) $
# $Rev: 70934 $
#

from lxml import etree
from appsequence import Applicability, StepBase


class LP_020200_DL_SeparateSpecsDisabled(StepBase):
  name = 'LP_020200_DL_SeparateSpecsDisabled'
  description = 'LP_020200_DL_SeparateSpecsDisabled'
  version = '$Rev: 70934 $'

  def __init__(self, forceApply=False):
    super(LP_020200_DL_SeparateSpecsDisabled, self).__init__(forceApply)

  def getApplicability(self, appseq):

    lgm = appseq.getLoadGroupMgr()
    if not lgm.hasLoadGroup('ExtractAnalyticAttributesIntoSourceTable'):
      return Applicability.cannotApplyPass
    return Applicability.canApply

  def apply(self, appseq):

    success = False

    lgm = appseq.getLoadGroupMgr()
    type = appseq.getText('template_type')

# Set Specs for Q_Unpivot_* Queries DL Load Group ExtractAnalyticAttributesIntoSourceTable
    pltd = lgm.getLoadGroup('ExtractAnalyticAttributesIntoSourceTable')

    if  type == 'MKTO':
      step_xml='''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteLeadAnalyticAttribute" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Lead_ID" itcn="SFDCLeadID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteContactAnalyticAttribute" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Contact_ID" itcn="SFDCContactID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    elif  type == 'ELQ':
      step_xml='''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteLeadAnalyticAttribute" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Lead_ID" itcn="SFDCLeadID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteContactAnalyticAttribute" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Contact_ID" itcn="SFDCContactID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    else:
      step_xml='''<extractQueries>
        <extractQuery qw="Workspace" queryName="Q_Unpivot_By_SFDC_Lead_Contact_ID_PLS_Scoring_Incremental" queryAlias="Q_Unpivot_By_SFDC_Lead_Contact_ID_PLS_Scoring_Incremental" sw="Workspace" schemaName="DanteLeadAnalyticAttribute" at="False" ucm="True">
          <schemas />
          <specs />
          <cms>
            <cm qcn="SFDC_Lead_Contact_ID" itcn="SFDCLeadID" />
            <cm qcn="AttributeName" itcn="AttributeName" />
            <cm qcn="AttributeValue" itcn="AttributeValue" />
          </cms>
        </extractQuery>
      </extractQueries>'''

    lgm.setLoadGroupFunctionality( 'ExtractAnalyticAttributesIntoSourceTable', step_xml )

    success = True

    return success
