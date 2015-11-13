
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from lxml import etree
from liaison import *
from appsequence import Applicability, AppSequence, StepBase

class LP_020100_DisableCreateBIQueries( StepBase ):

    name        = 'LP_020100_DisableCreateBIQueries'
    description = 'Disable CreateBIQueries; limit existing BI queries to 50 attributes'
    version     = '$Rev$'

    ## Add 5 to the maximum number of columns desired to account for 2 entities, a boundary,
    ## AttributeName and AttributeValue
    MAX_COLS    = 55


    def __init__( self, forceApply = False ):
        super( LP_020100_DisableCreateBIQueries, self ).__init__( forceApply )


    def getApplicability( self, appseq ):
        lg_mgr = appseq.getLoadGroupMgr()
        
        if not lg_mgr.hasLoadGroup( 'InsightsAllSteps' ):
            return Applicability.cannotApplyFail

        ias_ngs_xml = lg_mgr.getLoadGroupFunctionality( 'InsightsAllSteps', 'ngs' )
        ias_ngs = etree.fromstring( ias_ngs_xml )
        for ng in ias_ngs:
            if ng.get('n') == 'CreateBIQueries':
                return Applicability.canApply

        return Applicability.cannotApplyPass


    def apply( self, appseq ):
        template_type = appseq.getText( 'template_type' )
        conn_mgr = appseq.getConnectionMgr()
        lg_mgr = appseq.getLoadGroupMgr()
        
        ias_ngs_xml = lg_mgr.getLoadGroupFunctionality( 'InsightsAllSteps', 'ngs' )
        ias_ngs = etree.fromstring( ias_ngs_xml )
        for ng in ias_ngs:
            if ng.get('n') == 'CreateBIQueries':
                ias_ngs.remove( ng )

        lg_mgr.setLoadGroupFunctionality( 'InsightsAllSteps', etree.tostring(ias_ngs) )

        queries = []

        if template_type == 'SFDC':
            q_unpivot = conn_mgr.getQuery('Q_Unpivot_By_SFDC_Lead_Contact_ID_PLS_Scoring_Incremental')
            queries.append(q_unpivot)
        else:
            q_unpivot_contact = conn_mgr.getQuery('Q_Unpivot_By_SFDC_Contact_ID_PLS_Scoring_Incremental')
            q_unpivot_lead = conn_mgr.getQuery('Q_Unpivot_By_SFDC_Lead_ID_PLS_Scoring_Incremental')
            queries.append(q_unpivot_contact)
            queries.append(q_unpivot_lead)

        for q in queries:
            cols = q.getColumns()
            if len(cols) > self.MAX_COLS:
                cols = cols[:self.MAX_COLS]
            q.setColumns(cols)
            conn_mgr.setQuery(q)
        
        return True
