package com.latticeengines.datacloud.match.actors.visitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;

import akka.actor.ActorRef;

public abstract class DataSourceWrapperActorTemplate extends ActorTemplate {

    private static final Log log = LogFactory.getLog(DataSourceWrapperActorTemplate.class);

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

            if (shouldDoAsyncLookup()) {
                String lookupId = request.getMatchTravelerContext().getTravelerId();
                requestMap.put(lookupId, request);
                if (log.isDebugEnabled()) {
                    log.debug(self() + " received an async request from " + sender());
                }
                dataSourceLookupService.asyncLookup(lookupId, request, self().path().toSerializationFormat());
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(self() + " received a sync request from " + sender());
                }
                Response response = dataSourceLookupService.syncLookup(request);
                response.setTravelerContext(request.getMatchTravelerContext());
                if (log.isDebugEnabled()) {
                    log.debug(self() + " is sending back a sync response to " + sender());
                }
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
        if (log.isDebugEnabled()) {
            log.debug(self() + " is sending back an async response to " + callerMicroEngineActorRef);
        }
        callerMicroEngineActorRef.tell(response, self());
    }
}
