package com.latticeengines.actors.visitor.sample;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchActorSystem;

import akka.actor.ActorRef;

public abstract class SampleDataSourceWrapperActorTemplate extends ActorTemplate {

    private static Map<String, SampleDataSourceLookupRequest> requestMap = new HashMap<>();

    @Autowired
    protected SampleMatchActorSystem sampleMatchActorSystem;

    protected abstract SampleDataSourceLookupService getDataSourceLookupService();

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof SampleDataSourceLookupRequest || msg instanceof Response;
    }

    @Override
    protected void processMessage(Object msg) {
        if (msg instanceof SampleDataSourceLookupRequest) {
            SampleDataSourceLookupRequest request = (SampleDataSourceLookupRequest) msg;
            request.setCallerMicroEngineReference(ActorUtils.getPath(sender()));

            SampleDataSourceLookupService dataSourceLookupService = getDataSourceLookupService();

            if (shouldDoAsyncLookup()) {
                String lookupId = UUID.randomUUID().toString();
                requestMap.put(lookupId, request);
                dataSourceLookupService.asyncLookup(lookupId, request, ActorUtils.getPath(self()));
            } else {
                Response response = dataSourceLookupService.syncLookup(request);
                response.setTravelerContext(request.getMatchTravelerContext());
                sender().tell(response, self());
            }
        } else if (msg instanceof Response) {
            Response response = (Response) msg;
            String lookupId = response.getRequestId();
            SampleDataSourceLookupRequest request = requestMap.remove(lookupId);
            response.setTravelerContext(request.getMatchTravelerContext());

            sendResponseToCaller(request, response);
        } else {
            unhandled(msg);
        }

    }

    protected boolean shouldDoAsyncLookup() {
        return false;
    }

    @SuppressWarnings("deprecation")
    private void sendResponseToCaller(SampleDataSourceLookupRequest request, Response response) {
        ActorRef callerMicroEngineActorRef = context().actorFor(request.getCallerMicroEngineReference());
        callerMicroEngineActorRef.tell(response, self());
    }
}
