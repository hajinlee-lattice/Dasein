package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelException;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

public abstract class DataSourceLookupServiceBase implements DataSourceLookupService {
    private static final Log log = LogFactory.getLog(DataSourceLookupServiceBase.class);

    @Autowired
    private MatchActorSystem actorSystem;

    private final ConcurrentMap<String, String> reqReturnAddrs = new ConcurrentHashMap<String, String>();

    private final ConcurrentMap<String, DataSourceLookupRequest> reqs = new ConcurrentHashMap<>();

    abstract protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request);

    abstract protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress);

    @Override
    public void asyncLookup(String lookupRequestId, Object request, String returnAddress) {
        ExecutorService executor = actorSystem.getDataSourceServiceExecutor();
        Runnable task = createLookupRunnable(lookupRequestId, request, returnAddress);
        executor.execute(task);
    }

    private Runnable createLookupRunnable(final String lookupRequestId, final Object request,
            final String returnAddress) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                if (request instanceof DataSourceLookupRequest) {
                    asyncLookupFromService(lookupRequestId, (DataSourceLookupRequest) request, returnAddress);
                } else {
                    sendResponse(lookupRequestId, null, returnAddress);
                    log.error("Request type is not supported in DataSourceLookupService");
                }
            }
        };
        return task;
    }

    @Override
    public Response syncLookup(Object request) {
        Response response = new Response();
        if (request instanceof DataSourceLookupRequest) {
            Object result = lookupFromService(null, (DataSourceLookupRequest) request);
            response.setResult(result);
        }
        return response;
    }

    protected void sendResponse(String lookupRequestId, Object result, String returnAddress) {
        Response response = new Response();
        response.setRequestId(lookupRequestId);
        response.setResult(result);
        if (log.isDebugEnabled()) {
            log.debug("Returned response for " + lookupRequestId + " to " + returnAddress);
        }
        actorSystem.sendResponse(response, returnAddress);
    }

    protected void sendFailureResponse(DataSourceLookupRequest request, Exception ex) {
        Response response = new Response();
        response.setTravelerContext(request.getMatchTravelerContext());
        log.error(String.format("Encountered issue at %s for request %s: %s",
                DataSourceWrapperActorTemplate.class.getSimpleName(), request.getMatchTravelerContext().getTravelerId(),
                ex.getMessage()));
        response.getTravelerContext().setTravelException(new TravelException(ex.getMessage(), ex));
        actorSystem.sendResponse(response, request.getMatchTravelerContext().getAnchorActorLocation());
    }

    protected boolean isBatchMode() {
        return actorSystem.isBatchMode();
    }

    protected void saveReq(String lookupRequestId, String returnAddr, DataSourceLookupRequest req) {
        reqReturnAddrs.put(lookupRequestId, returnAddr);
        reqs.put(lookupRequestId, req);
    }

    protected void removeReq(String lookRequestId) {
        reqReturnAddrs.remove(lookRequestId);
        reqs.remove(lookRequestId);
    }

    protected String getReqReturnAdd(String lookupRequestId) {
        return reqReturnAddrs.get(lookupRequestId);
    }

    protected DataSourceLookupRequest getReq(String lookupRequestId) {
        return reqs.get(lookupRequestId);
    }

    protected MatchActorSystem getActorSystem() {
        return actorSystem;
    }
}
