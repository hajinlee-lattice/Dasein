package com.latticeengines.datacloud.match.actors.visitor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;

import akka.actor.ActorRef;

public abstract class DataSourceWrapperActorTemplate extends ActorTemplate {

    @Autowired
    @Qualifier("matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    private static Map<String, DataSourceLookupRequest> requestMap = new HashMap<>();

    protected abstract DataSourceLookupService getDataSourceLookupService();

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof DataSourceLookupRequest || msg instanceof Response;
    }

    @Override
    protected void processMessage(Object msg) {
        if (msg instanceof DataSourceLookupRequest) {
            DataSourceLookupRequest request = (DataSourceLookupRequest) msg;
            request.setCallerMicroEngineReference(sender().path().toSerializationFormat());

            DataSourceLookupService dataSourceLookupService = getDataSourceLookupService();
            MatchTraveler traveler = request.getMatchTravelerContext();

            if (shouldDoAsyncLookup()) {
                String lookupId = UUID.randomUUID().toString();
                requestMap.put(lookupId, request);
                traveler.debug(getClass().getSimpleName() + " received an async request for " + traveler + " from "
                        + matchActorSystem.getActorName(sender()));
                dataSourceLookupService.asyncLookup(lookupId, request, self().path().toSerializationFormat());
            } else {
                traveler.debug(getClass().getSimpleName() + " received a sync request for " + traveler + " from "
                        + matchActorSystem.getActorName(sender()));
                Response response = dataSourceLookupService.syncLookup(request);
                response.setTravelerContext(request.getMatchTravelerContext());
                traveler.debug(getClass().getSimpleName() + " sent back a sync response for " + traveler + " to "
                        + matchActorSystem.getActorName(sender()));
                sender().tell(response, self());
            }
        } else if (msg instanceof Response) {
            Response response = (Response) msg;
            String lookupId = response.getRequestId();
            DataSourceLookupRequest request = requestMap.remove(lookupId);
            response.setTravelerContext(request.getMatchTravelerContext());
            MatchTraveler traveler = request.getMatchTravelerContext();
            traveler.debug(getClass().getSimpleName() + " sent back an async response for " + traveler + " to "
                    + matchActorSystem.getActorName(sender()));
            sendResponseToCaller(request, response);
        } else {
            unhandled(msg);
        }

    }

    protected boolean shouldDoAsyncLookup() {
        return false;
    }

    private void sendResponseToCaller(DataSourceLookupRequest request, Response response) {
        ActorRef callerMicroEngineActorRef = context().actorFor(request.getCallerMicroEngineReference());
        callerMicroEngineActorRef.tell(response, self());
    }
}
