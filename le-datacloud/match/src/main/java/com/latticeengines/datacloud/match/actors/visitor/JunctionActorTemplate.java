package com.latticeengines.datacloud.match.actors.visitor;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

import akka.actor.ActorRef;

public abstract class JunctionActorTemplate extends VisitorActorTemplate {

    @Autowired
    @Qualifier("matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

    protected abstract boolean accept(Traveler traveler);

    @Override
    public GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler || msg instanceof Response;
    }

    @Override
    protected boolean process(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        guideBook.logVisit(ActorUtils.getPath(self()), matchTraveler);
        if (accept(matchTraveler)) {
            DecisionGraph nextDG = null;
            try {
                nextDG = matchDecisionGraphService.findNextDecisionGraphForJunction(matchTraveler.getDecisionGraph(),
                        getClass().getSimpleName());
            } catch (ExecutionException e) {
                // TODO: After moving decision graph concept to
                // le-actor project, here we should return to traveler's
                // original location directly. No need to continue traveling
                matchTraveler.warn("Rejected by " + getClass().getSimpleName()
                        + " due to fail to retrieve jump-to decision graph for junction " + getClass().getSimpleName());
                return false;
            }
            ActorRef nextAnchorRef = matchActorSystem.getActorRef(nextDG.getAnchor());
            setupTravelerBeforeJump(matchTraveler, nextDG.getGraphName());
            nextAnchorRef.tell(matchTraveler, self());
            return true;
        } else {
            matchTraveler.debug("Rejected by " + getClass().getSimpleName());
            return false;
        }
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        recoverTraveler(traveler);
    }

    private void setupTravelerBeforeJump(MatchTraveler traveler, String nextDGName) {
        traveler.setProcessed(Boolean.FALSE);
        traveler.pushToTransitionHistory(getClass().getSimpleName(), traveler.getDecisionGraph(), true);
        traveler.setDecisionGraph(nextDGName);
    }

    private void recoverTraveler(MatchTraveler traveler) {
        traveler.setProcessed(Boolean.TRUE);
        String originalDGName = traveler.recoverTransitionHistory();
        DecisionGraph originalDG = null;
        try {
            originalDG = matchDecisionGraphService.getDecisionGraph(originalDGName);
        } catch (ExecutionException e) {
            // TODO: After moving decision graph concept to le-actor
            // project, here we should return to traveler's original location
            // Currently traveler will be lost because original decision graph
            // cannot be recover
            return;
        }
        traveler.setDecisionGraph(originalDGName);
        traveler.setAnchorActorLocation(ActorUtils.getPath(matchActorSystem.getActorRef(originalDG.getAnchor())));
        traveler.setMatched(Boolean.FALSE);
    }
}
