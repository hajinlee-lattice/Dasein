package com.latticeengines.datacloud.match.actors.framework.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.entitymgr.DecisionGraphEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

@Component("matchDecisionGraphService")
public class MatchDecisionGraphServiceImpl implements MatchDecisionGraphService {

    @Autowired
    private DecisionGraphEntityMgr decisionGraphEntityMgr;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    private LoadingCache<String, DecisionGraph> decisionGraphLoadingCache;

    @PostConstruct
    private void postConstruct() {
        decisionGraphLoadingCache = CacheBuilder.newBuilder().maximumSize(20).expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, DecisionGraph>() {
                    @Override
                    public DecisionGraph load(String graphName) throws Exception {
                        return decisionGraphEntityMgr.getDecisionGraph(graphName);
                    }
                });
    }

    @Override
    public DecisionGraph getDecisionGraph(MatchTraveler traveler) throws ExecutionException {
        String graphName = traveler.getDecisionGraph();
        if (StringUtils.isEmpty(graphName)) {
            graphName = defaultGraph;
            traveler.setDecisionGraph(defaultGraph);
        }
        return decisionGraphLoadingCache.get(graphName);
    }

    @Override
    public DecisionGraph getDecisionGraph(String graphName) throws ExecutionException {
        if (StringUtils.isEmpty(graphName)) {
            graphName = defaultGraph;
        }
        return decisionGraphLoadingCache.get(graphName);
    }

    @Override
    public DecisionGraph findNextDecisionGraphForJunction(String currentGraphName, String junctionName)
            throws ExecutionException {
        DecisionGraph decisionGraph = getDecisionGraph(currentGraphName);
        String jumpToGraphName = decisionGraph.getNextGraphForJunction(junctionName);
        return getDecisionGraph(jumpToGraphName);
    }

}
