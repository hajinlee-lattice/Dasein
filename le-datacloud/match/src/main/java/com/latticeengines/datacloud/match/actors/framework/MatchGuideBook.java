package com.latticeengines.datacloud.match.actors.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.entitymgr.DecisionGraphEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

@Component("matchGuideBook")
public class MatchGuideBook extends GuideBook {

    private static final Logger log = LoggerFactory.getLogger(MatchGuideBook.class);
    private static final String MICROENGINE_ACTOR = "MicroEngineActor";
    public static final String DEFAULT_GRAPH = null;

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
    }

    @Override
    public String next(String currentLocation, Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        if (fuzzyMatchAnchorPath.equals(currentLocation)) {
            return nextMoveForAnchor(matchTraveler);
        } else {
            String nextStop = nextMoveForMicroEngine(matchTraveler);
            if (fuzzyMatchAnchorPath.equals(nextStop)) {
                String lastStop = toActorName(actorSystem.getActorName(currentLocation));
                matchTraveler.setLastStop(lastStop);
            }
            return nextStop;
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
            traveler.warn("Failed to retrieve decision graph " + ((MatchTraveler) traveler).getDecisionGraph()
                    + " from loading cache.", e);
            traveler.clearLocationsToVisitingQueue();
            return;
        }

        String nodeName = toActorName(actorSystem.getActorName(traversedActor));
        DecisionGraph.Node thisNode = decisionGraph.getNode(nodeName);
        if (thisNode == null) {
            throw new RuntimeException("Cannot find the graph node named " + nodeName);
        }
        List<DecisionGraph.Node> children = thisNode.getChildren();
        List<String> childNodes = new ArrayList<>();
        for (DecisionGraph.Node child : children) {
            String actorPath = actorSystem.getActorRef(toActorClassName(child.getName())).path()
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
                traveler.warn(
                        "Failed to retrieve decision graph " + traveler.getDecisionGraph() + " from loading cache.", e);
                return traveler.getOriginalLocation();
            }

            String[] startingNodes = new String[decisionGraph.getStartingNodes().size()];
            for (int i = 0; i < startingNodes.length; i++) {
                DecisionGraph.Node node = decisionGraph.getStartingNodes().get(i);
                String actorPath = actorSystem.getActorRef(toActorClassName(node.getName())).path()
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
            traveler.debug("Has no where else to go but heading back to home.");
        }

        return traveler.getAnchorActorLocation();
    }

    private boolean visitSameMicroEngineWithSameDataAgain(String candidateDestination, MatchTraveler traveler) {
        Map<String, Set<String>> history = traveler.getVisitedHistory();
        String currentContext = traveler.getMatchKeyTuple().toString();
        if (StringUtils.isNotEmpty(candidateDestination) && history.containsKey(candidateDestination)) {
            Set<String> previousData = history.get(candidateDestination);
            if (previousData.contains(currentContext)) {
                traveler.debug("Skipping " + actorSystem.getActorName(candidateDestination)
                        + " because this is the second visit with the same context.");
                return true;
            }
        }
        return false;
    }

    private DecisionGraph getDecisionGraph(MatchTraveler traveler) throws ExecutionException {
        String graphName = traveler.getDecisionGraph();
        if (StringUtils.isEmpty(graphName)) {
            graphName = defaultGraph;
            traveler.setDecisionGraph(defaultGraph);
        }
        return decisionGraphLoadingCache.get(graphName);
    }

    private String toActorName(String actorClassName) {
        return actorClassName.replace(MICROENGINE_ACTOR, "");
    }

    private String toActorClassName(String actorName) {
        return actorName + MICROENGINE_ACTOR;
    }

}
