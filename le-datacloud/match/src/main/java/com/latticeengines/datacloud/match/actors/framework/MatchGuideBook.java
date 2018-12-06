package com.latticeengines.datacloud.match.actors.framework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;

@Component("matchGuideBook")
public class MatchGuideBook extends GuideBook {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchGuideBook.class);

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

    @Override
    public String next(String currentLocation, Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        String anchorPath;
        try {
            anchorPath = actorSystem
                    .getAnchorPath(matchDecisionGraphService.getDecisionGraph(matchTraveler).getAnchor());
        } catch (ExecutionException e) {
            traveler.warn("Fail to retrieve anchor path for match traveler " + JsonUtils.serialize(matchTraveler), e);
            return traveler.getOriginalLocation();
        }

        if (anchorPath.equals(currentLocation)) {
            return nextMoveForAnchor(matchTraveler);
        } else {
            String nextStop = nextMoveForMicroEngine(matchTraveler);
            if (anchorPath.equals(nextStop)) {
                matchTraveler.setLastStop(actorSystem.getActorName(currentLocation));
            }
            return nextStop;
        }
    }

    @Override
    public void logVisit(String traversedActor, Traveler traveler) {
        DecisionGraph decisionGraph;
        try {
            decisionGraph = matchDecisionGraphService.getDecisionGraph((MatchTraveler) traveler);
        } catch (Exception e) {
            traveler.warn("Failed to retrieve decision graph " + ((MatchTraveler) traveler).getDecisionGraph()
                    + " from loading cache.", e);
            traveler.clearLocationsToVisitingQueue();
            return;
        }
        String anchorPath = actorSystem.getAnchorPath(decisionGraph.getAnchor());
        if (anchorPath.equals(traversedActor)) {
            return;
        }

        traveler.logVisitHistory(traversedActor);

        // Node.name is actor name abbreviation, not full name
        String nodeName = toActorNameAbbr(actorSystem.getActorName(traversedActor));
        DecisionGraph.Node thisNode = decisionGraph.getNode(nodeName);
        if (thisNode == null) {
            throw new RuntimeException("Cannot find the graph node named " + nodeName);
        }
        List<DecisionGraph.Node> children = thisNode.getChildren();
        List<String> childNodes = new ArrayList<>();
        for (DecisionGraph.Node child : children) {
            String actorPath = ActorUtils.getPath(actorSystem.getActorRef(toActorName(child.getName())));
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
                decisionGraph = matchDecisionGraphService.getDecisionGraph(traveler);
            } catch (Exception e) {
                traveler.warn(
                        "Failed to retrieve decision graph " + traveler.getDecisionGraph() + " from loading cache.", e);
                return traveler.getOriginalLocation();
            }

            String[] startingNodes = new String[decisionGraph.getStartingNodes().size()];
            for (int i = 0; i < startingNodes.length; i++) {
                DecisionGraph.Node node = decisionGraph.getStartingNodes().get(i);
                String actorPath = ActorUtils.getPath(actorSystem.getActorRef(toActorName(node.getName())));
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

    private String toActorNameAbbr(String actorName) {
        return MatchActorUtils.getShortActorName(actorName, actorSystem.getActorType(actorName));
    }

    private String toActorName(String actorNameAbbr) {
        return MatchActorUtils.getFullActorName(actorNameAbbr, actorSystem.getActorTypeByNameAbbr(actorNameAbbr));
    }

}
