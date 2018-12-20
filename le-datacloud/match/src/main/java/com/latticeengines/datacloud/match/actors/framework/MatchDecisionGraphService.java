package com.latticeengines.datacloud.match.actors.framework;

import java.util.concurrent.ExecutionException;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

public interface MatchDecisionGraphService {
    DecisionGraph getDecisionGraph(MatchTraveler traveler) throws ExecutionException;

    DecisionGraph getDecisionGraph(String graphName) throws ExecutionException;

    DecisionGraph findNextDecisionGraphForJunction(String currentGraphName, String junctionName)
            throws ExecutionException;
}
