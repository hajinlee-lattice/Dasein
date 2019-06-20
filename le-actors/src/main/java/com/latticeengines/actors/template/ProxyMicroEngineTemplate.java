package com.latticeengines.actors.template;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;

/**
 * Actors in decision graph have 3 types: anchor, micro-engine & junction
 * 
 * Anchor is entry/exit actor
 * 
 * Micro-engine is actors where traveler travels around. Micro-engine has 2
 * types: proxy micro-engine & task micro-engine. Proxy micro-engine needs
 * assistant actor to finish task. Executor micro-engine does the task by
 * itself.
 * 
 * Junction is the connecting point between decision graph/actor system
 */
public abstract class ProxyMicroEngineTemplate extends VisitorActorTemplate {

    /**
     * Proxy micro-engine actor needs assistant actor to finish task. Override
     * this method to send request to assistant actor
     * 
     * @param traveler
     */
    protected abstract void sendReqToAssistantActor(Traveler traveler);

    /*
     * Proxy micro-engine actor needs assistant actor to finish task. Override
     * this method to process response from assistant actor
     */
    @Override
    protected abstract void process(Response response);

    @Override
    protected boolean needAssistantActor() {
        return true;
    }

    @Override
    protected boolean process(Traveler traveler) {
        // Inject failure only for testing purpose
        injectFailure(traveler);
        if (accept(traveler)) {
            sendReqToAssistantActor(traveler);
            return true;
        } else {
            traveler.debug("Rejected by " + getActorSystem().getActorName(self()));
            return false;
        }
    }
}
