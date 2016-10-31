package com.latticeengines.datacloud.match.actors.visitor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;

import akka.actor.ActorRef;

public abstract class DataSourceWrapperActorTemplate extends ActorTemplate {

    private static Map<String, DataSourceLookupRequest> requestMap = new HashMap<>();

    protected abstract DataSourceLookupService getDataSourceLookupService();

    protected abstract GuideBook getGuideBook();

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

            if (shouldDoAsyncLookup()) {
                String lookupId = UUID.randomUUID().toString();
                requestMap.put(lookupId, request);
                dataSourceLookupService.asyncLookup(lookupId, request.getInputData(),
                        self().path().toSerializationFormat());
            } else {
                Response response = dataSourceLookupService.syncLookup(request.getInputData());
                response.setTravelerContext(request.getMatchTravelerContext());
                sender().tell(response, self());
            }
        } else if (msg instanceof Response) {
            Response response = (Response) msg;
            String lookupId = response.getRequestId();
            DataSourceLookupRequest request = requestMap.remove(lookupId);
            response.setTravelerContext(request.getMatchTravelerContext());

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
