package com.latticeengines.datacloud.match.actors.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.TravelWarning;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.entitymgr.DecisionGraphEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

@Component("matchGuideBook")
public class MatchGuideBook extends GuideBook {

    private static final Log log = LogFactory.getLog(MatchGuideBook.class);
    private static final String MICROENGINE_ACTOR = "MicroEngineActor";

    private String fuzzyMatchAnchorPath;

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private DecisionGraphEntityMgr decisionGraphEntityMgr;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    private LoadingCache<String, DecisionGraph> decisionGraphLoadingCache;

    @PostConstruct
    public void init() {
        log.info("Initialize fuzzy match guide book.");
        fuzzyMatchAnchorPath = actorSystem.getFuzzyMatchAnchor().path().toSerializationFormat();

        decisionGraphLoadingCache = CacheBuilder.newBuilder().maximumSize(20).expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, DecisionGraph>() {
                    @Override
                    public DecisionGraph load(String graphName) throws Exception {
                        return decisionGraphEntityMgr.getDecisionGraph(graphName);
                    }
                });
        try {
            decisionGraphLoadingCache.get(defaultGraph);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to load default decision graph " + defaultGraph + " into loading cache",
                    e);
        }
    }

    @Override
    public String next(String currentLocation, Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        if (fuzzyMatchAnchorPath.equals(currentLocation)) {
            return nextMoveForAnchor(matchTraveler);
        } else {
            return nextMoveForMicroEngine(matchTraveler);
        }
    }

    @Override
    public void logVisit(String traversedActor, Traveler traveler) {
        if (fuzzyMatchAnchorPath.equals(traversedActor)) {
            return;
        }

        traveler.logVisitHistory(traversedActor);
        DecisionGraph decisionGraph;
        try {
            decisionGraph = getDecisionGraph((MatchTraveler) traveler);
        } catch (Exception e) {
            traveler.getTravelWarnings().add(new TravelWarning("Failed to retrieve decision graph "
                    + ((MatchTraveler) traveler).getDecisionGraph() + " from loading cache."));
            traveler.clearLocationsToVisitingQueue();
            return;
        }

        String nodeName = actorSystem.getActorClassName(traversedActor).replace(MICROENGINE_ACTOR, "");
        DecisionGraph.Node thisNode = decisionGraph.getNode(nodeName);
        if (thisNode == null) {
            log.error("Cannot find node named " + nodeName);
        }
        List<DecisionGraph.Node> children = thisNode.getChildren();
        List<String> childNodes = new ArrayList<>();
        for (DecisionGraph.Node child : children) {
            String actorPath = actorSystem.getActorRef(child.getName() + MICROENGINE_ACTOR).path()
                    .toSerializationFormat();
            childNodes.add(actorPath);
        }
        childNodes.removeAll(traveler.getVisitingQueue());
        traveler.addLocationsToVisitingQueue(childNodes.toArray(new String[childNodes.size()]));
    }

    private String nextMoveForAnchor(MatchTraveler traveler) {
        if (!traveler.isProcessed()) {
            traveler.setProcessed(true);

            // initialization
            DecisionGraph decisionGraph;
            try {
                decisionGraph = getDecisionGraph(traveler);
            } catch (Exception e) {
                traveler.getTravelWarnings().add(new TravelWarning(
                        "Failed to retrieve decision graph " + traveler.getDecisionGraph() + " from loading cache."));
                return traveler.getOriginalLocation();
            }

            String[] startingNodes = new String[decisionGraph.getStartingNodes().size()];
            for (int i = 0; i < startingNodes.length; i++) {
                DecisionGraph.Node node = decisionGraph.getStartingNodes().get(i);
                String actorPath = actorSystem.getActorRef(node.getName() + MICROENGINE_ACTOR).path()
                        .toSerializationFormat();
                startingNodes[i] = actorPath;
            }

            traveler.addLocationsToVisitingQueue(startingNodes);
            return traveler.getNextLocationFromVisitingQueue();
        } else {
            return traveler.getOriginalLocation();
        }
    }

    private String nextMoveForMicroEngine(MatchTraveler traveler) {
        String destinationLocation;

        if (!traveler.isMatched()) {
            do {
                destinationLocation = traveler.getNextLocationFromVisitingQueue();
                if (!visitSameMicroEngineWithSameDataAgain(destinationLocation, traveler)) {
                    return destinationLocation;
                }
            } while (StringUtils.isNotEmpty(destinationLocation));
        }

        return traveler.getAnchorActorLocation();
    }

    private boolean visitSameMicroEngineWithSameDataAgain(String candidateDestination, MatchTraveler traveler) {
        Map<String, Set<String>> history = traveler.getVisitedHistory();
        String currentContext = JsonUtils.serialize(traveler.getMatchKeyTuple());
        if (StringUtils.isNotEmpty(candidateDestination) && history.containsKey(candidateDestination)) {
            Set<String> previousData = history.get(candidateDestination);
            return previousData.contains(currentContext);
        } else {
            return false;
        }
    }

    private DecisionGraph getDecisionGraph(MatchTraveler traveler) throws ExecutionException {
        String graphName = traveler.getDecisionGraph();
        if (StringUtils.isEmpty(graphName)) {
            graphName = defaultGraph;
            traveler.setDecisionGraph(defaultGraph);
        }
        return decisionGraphLoadingCache.get(graphName);
    }

}
