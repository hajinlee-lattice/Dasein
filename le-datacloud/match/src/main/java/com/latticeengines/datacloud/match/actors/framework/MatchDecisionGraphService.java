package com.latticeengines.datacloud.match.actors.framework;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

public interface MatchDecisionGraphService {
    DecisionGraph getDecisionGraph(MatchTraveler traveler) throws ExecutionException;

    DecisionGraph getDecisionGraph(String graphName) throws ExecutionException;

    /**
     * Plan to replace getDecisionGraph with findDecisionGraph by not exposing
     * ExecutionException to caller.
     * 
     * Currently if not providing graphName, findDecisionGraph() does not return
     * default decision graph. getDecisionGraph() returns default decision graph
     * for ldc match. Need to consider how to merge and handle default decision
     * graph for both entity mach and ldc match
     * 
     * @param graphName
     * @return
     */
    DecisionGraph findDecisionGraph(String graphName);

    DecisionGraph findNextDecisionGraphForJunction(String currentGraphName, String junctionName)
            throws ExecutionException;

    List<DecisionGraph> findAll();
}
