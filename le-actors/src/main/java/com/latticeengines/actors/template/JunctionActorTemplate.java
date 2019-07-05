package com.latticeengines.actors.template;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

/**
 * Actors in decision graph have 3 types: anchor, micro-engine & junction
 * 
 * Anchor is entry/exit actor
 * 
 * Micro-engine is actors where traveler travels around
 * 
 * Junction is the connecting point between decision graph/actor system
 */
public abstract class JunctionActorTemplate extends VisitorActorTemplate {

    /***********************************
     * Methods to override (optionally)
     ***********************************/
    protected abstract DecisionGraph findNextDecisionGraph(String currentDecisionGraph);

    protected void prepareTravelerBeforeTransfer(Traveler traveler) {

    }

    protected void prepareTravelerAfterTransfer(Traveler traveler) {

    }

    /*******************
     * Business methods
     *******************/

    @Override
    protected boolean needAssistantActor() {
        return false;
    }


    @Override
    protected boolean process(Traveler traveler) {
        try {
            if (accept(traveler)) {
                DecisionGraph nextDG = findNextDecisionGraph(traveler.getDecisionGraph());
                setupTravelerBeforeTransfer(traveler, nextDG.getGraphName());
                getActorSystem().getAnchor().tell(traveler, self());
                return true;
            } else {
                traveler.debug("Rejected by " + getActorName(self()));
                return false;
            }
        } catch (Exception e) {
            traveler.error(
                    String.format("Force to return anchor due to exception encountered at %s: %s",
                            getActorName(self()), e.getMessage()),
                    e);
            forceReturnToAnchor(traveler);
            return true;
        }
    }

    @Override
    protected void process(Response response) {
        Traveler traveler = response.getTravelerContext();
        // Inject failure only for testing purpose
        injectFailure(traveler);
        recoverTraveler(traveler);
    }

    private void setupTravelerBeforeTransfer(Traveler traveler, String nextDGName) {
        traveler.setProcessed(false);
        traveler.pushToTransitionHistory(getActorName(self()), true);
        traveler.setDecisionGraph(nextDGName);
        traveler.clearResult();
        prepareTravelerBeforeTransfer(traveler);
    }

    private void recoverTraveler(Traveler traveler) {
        traveler.setProcessed(true);
        traveler.recoverTransitionHistory();
        traveler.setAnchorActorLocation(ActorUtils.getPath(getActorSystem().getAnchor()));
        traveler.clearResult();
        prepareTravelerAfterTransfer(traveler);
    }
}
