package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.JunctionActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

/**
 * Junction actor to jump to LDC fuzzy match
 *
 */
@Component("matchJunctionActor")
@Scope("prototype")
public class MatchJunctionActor extends JunctionActorTemplate {

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Autowired
    private MatchActorSystem matchActorSystem;

    @Override
    public GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return matchActorSystem;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler || msg instanceof Response;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        DecisionGraph decisionGraph = findDecisionGraphByName(traveler.getDecisionGraph());
        return StringUtils.isNotBlank(decisionGraph.getNextGraphForJunction(getCurrentActorName()));
    }

    @Override
    protected DecisionGraph findNextDecisionGraph(String currentDecisionGraph) {
        try {
            return matchDecisionGraphService.findNextDecisionGraphForJunction(currentDecisionGraph,
                    getCurrentActorName());
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    "Fail to retrieve jump-to decision graph for at junction " + getCurrentActorName()
                            + " by current decision graph " + currentDecisionGraph);
        }
    }

    @Override
    protected void prepareTravelerBeforeTransfer(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        matchTraveler.setMatched(Boolean.FALSE);

        // Initialize entity
        DecisionGraph decisionGraph = findDecisionGraphByName(matchTraveler.getDecisionGraph());
        matchTraveler.setEntity(decisionGraph.getEntity());
        // If MatchKeyTuple for entity of next DG is not prepared yet, it will
        // be prepared in planner actor in next DG
        // If MatchKeyTuple for entity of next DG is prepared, re-use the
        // MatchKeyTuple
        // If jump to LDC match, MatchKeyTuple for entity LatticeAccount is
        // prepared in AccountMatchPlanner actor
        matchTraveler.setMatchKeyTuple(matchTraveler.getEntityMatchKeyTuple(decisionGraph.getEntity()));
    }

    @Override
    protected void prepareTravelerAfterTransfer(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        matchTraveler.setMatched(Boolean.FALSE);

        // Recover entity (Decision graph has been recovered in
        // JunctionActorTemplate.recoverTraveler)
        DecisionGraph decisionGraph = findDecisionGraphByName(matchTraveler.getDecisionGraph());
        matchTraveler.setEntity(decisionGraph.getEntity());
        matchTraveler.setMatchKeyTuple(matchTraveler.getEntityMatchKeyTuple(decisionGraph.getEntity()));
    }

    private DecisionGraph findDecisionGraphByName(String decisionGraph) {
        try {
            return matchDecisionGraphService.getDecisionGraph(decisionGraph);
        } catch (ExecutionException e) {
            throw new RuntimeException("Fail to retrieve decision graph for at junction " + getCurrentActorName()
                    + " by name " + decisionGraph);
        }
    }

}
