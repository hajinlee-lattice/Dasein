package com.latticeengines.datacloud.match.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

public interface DecisionGraphEntityMgr {

    DecisionGraph getDecisionGraph(String graphName);

    List<DecisionGraph> findAll();

}
