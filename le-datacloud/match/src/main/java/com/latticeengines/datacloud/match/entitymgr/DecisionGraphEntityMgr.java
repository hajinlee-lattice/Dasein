package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

public interface DecisionGraphEntityMgr {

    DecisionGraph getDecisionGraph(String graphName);

}
