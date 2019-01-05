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
        DecisionGraph nextDG = findNextDecisionGraph(matchTraveler.getDecisionGraph());
        matchTraveler.setEntity(nextDG.getEntity());

        // TODO For LDC match, since match planner is still outside of actor
        // system, matchKeyTuple should be prepared before transferring to next
        // decision graph. Currently assume LDC match is always transferred from
        // Account match, so matchTraveler.matchKeyTuple is already prepared and
        // standardized
        // Next action: move match planner for LDC match into actor system too

    }

    @Override
    protected void prepareTravelerAfterTransfer(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        matchTraveler.setMatched(Boolean.FALSE);

        // Recover entity (Decision graph has been recovered in
        // JunctionActorTemplate.recoverTraveler)
        DecisionGraph decisionGraph = findDecisionGraphByName(matchTraveler.getDecisionGraph());
        matchTraveler.setEntity(decisionGraph.getEntity());
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
