package com.latticeengines.actors.exposed.traveler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;

public abstract class GuideBook {

    /*********************
     * Abstract methods
     *********************/

    protected abstract ActorSystemTemplate getActorSystem();

    /**
     * Whether have achieved goal; If so, stop travel
     * 
     * @param traveler
     * @return
     */
    protected abstract boolean stopTravel(Traveler traveler);

    /**
     * To detect whether traveler reaches same micro engine with same context.
     * If so, should stop travel.
     * 
     * TODO: Need to make change to support repeated runs within decision graph
     * 
     * @param traveler
     * @return
     */
    protected abstract String getSerializedTravelerContext(Traveler traveler);

    /**
     * Get current decision graph info by graph name
     * 
     * @param traveler
     * @return
     * @throws Exception
     */
    public abstract DecisionGraph getDecisionGraphByName(String decisionGraph) throws Exception;


    public String next(String currentLocation, Traveler traveler) {
        String anchorPath = ActorUtils.getPath(getActorSystem().getAnchor());
        if (anchorPath.equals(currentLocation)) {
            return nextMoveForAnchor(traveler);
        } else {
            String nextStop = nextMoveForMicroEngine(traveler);
            if (anchorPath.equals(nextStop)) {
                traveler.setLastStop(getActorSystem().getActorName(currentLocation));
            }
            return nextStop;
        }
    }

    public void logVisit(String traversedActor, Traveler traveler) {
        DecisionGraph decisionGraph;
        try {
            decisionGraph = getDecisionGraphByName(traveler.getDecisionGraph());
        } catch (Exception e) {
            traveler.error("Failed to retrieve decision graph " + traveler.getDecisionGraph(), e);
            traveler.clearLocationsToVisitingQueue();
            return;
        }
        String anchorPath = ActorUtils.getPath(getActorSystem().getAnchor());
        if (anchorPath.equals(traversedActor)) {
            return;
        }

        traveler.logVisitHistory(traversedActor);

        // Node.name is actor name abbreviation, not full name
        String nodeName = toActorNameAbbr(getActorSystem().getActorName(traversedActor));
        DecisionGraph.Node thisNode = decisionGraph.getNode(nodeName);
        if (thisNode == null) {
            throw new RuntimeException("Cannot find the graph node named " + nodeName);
        }
        List<DecisionGraph.Node> children = thisNode.getChildren();
        List<String> childNodes = new ArrayList<>();
        for (DecisionGraph.Node child : children) {
            String actorPath = ActorUtils.getPath(getActorSystem().getActorRef(toActorName(child.getName())));
            childNodes.add(actorPath);
        }
        childNodes.removeAll(traveler.getVisitingQueue());
        traveler.addLocationsToVisitingQueue(childNodes.toArray(new String[childNodes.size()]));
    }

    private String nextMoveForAnchor(Traveler traveler) {
        if (!traveler.isProcessed()) {
            traveler.setProcessed(true);

            DecisionGraph decisionGraph;
            try {
                decisionGraph = getDecisionGraphByName(traveler.getDecisionGraph());
            } catch (Exception e) {
                traveler.error(
                        "Failed to retrieve decision graph " + traveler.getDecisionGraph(), e);
                return traveler.getOriginalLocation();
            }

            String[] startingNodes = new String[decisionGraph.getStartingNodes().size()];
            for (int i = 0; i < startingNodes.length; i++) {
                DecisionGraph.Node node = decisionGraph.getStartingNodes().get(i);
                String actorPath = ActorUtils.getPath(getActorSystem().getActorRef(toActorName(node.getName())));
                startingNodes[i] = actorPath;
            }

            traveler.addLocationsToVisitingQueue(startingNodes);
            return traveler.findNextLocationFromVisitingQueue();
        } else {
            return traveler.getOriginalLocation();
        }
    }

    private String nextMoveForMicroEngine(Traveler traveler) {
        String destinationLocation;

        if (!stopTravel(traveler)) {
            do {
                destinationLocation = traveler.findNextLocationFromVisitingQueue();
                if (!visitSameMicroEngineWithSameDataAgain(destinationLocation, traveler)) {
                    return destinationLocation;
                }
            } while (StringUtils.isNotEmpty(destinationLocation));
            traveler.debug("Has no where else to go but heading back to home.");
        }

        return traveler.getAnchorActorLocation();
    }

    private boolean visitSameMicroEngineWithSameDataAgain(String candidateDestination, Traveler traveler) {
        Map<String, Set<String>> history = traveler.getVisitedHistory();
        String currentContext = getSerializedTravelerContext(traveler);
        if (StringUtils.isNotEmpty(candidateDestination) && history.containsKey(candidateDestination)) {
            Set<String> previousData = history.get(candidateDestination);
            if (previousData.contains(currentContext)) {
                traveler.debug("Skipping " + getActorSystem().getActorName(candidateDestination)
                        + " because this is the second visit with the same context.");
                return true;
            }
        }
        return false;
    }

    private String toActorNameAbbr(String actorName) {
        return MatchActorUtils.getShortActorName(actorName, getActorSystem().getActorType(actorName));
    }

    private String toActorName(String actorNameAbbr) {
        return MatchActorUtils.getFullActorName(actorNameAbbr, getActorSystem().getActorTypeByNameAbbr(actorNameAbbr));
    }
}
