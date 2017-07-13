package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelException;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

public abstract class DataSourceLookupServiceBase implements DataSourceLookupService {
    private static final Logger log = LoggerFactory.getLogger(DataSourceLookupServiceBase.class);

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
                    try {
                        ((DataSourceLookupRequest) request).setTimestamp(System.currentTimeMillis());
                        asyncLookupFromService(lookupRequestId, (DataSourceLookupRequest) request, returnAddress);
                    } catch (Exception ex) {
                        sendFailureResponse(((DataSourceLookupRequest) request), ex);
                    }
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
            ((DataSourceLookupRequest) request).setTimestamp(System.currentTimeMillis());
            Object result = lookupFromService(null, (DataSourceLookupRequest) request);
            response.setResult(result);
        }
        return response;
    }

    protected void sendResponse(String lookupRequestId, Object result, String returnAddress) {
        Response response = new Response();
        response.setRequestId(lookupRequestId);
        response.setResult(result);
        actorSystem.sendResponse(response, returnAddress);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Returned response for %s to %s", lookupRequestId, returnAddress));
        }
    }

    protected void sendFailureResponse(DataSourceLookupRequest request, Exception ex) {
        Response response = new Response();
        response.setTravelerContext(request.getMatchTravelerContext());
        log.error(String.format("Encountered issue at %s for request %s: %s",
                DataSourceWrapperActorTemplate.class.getSimpleName(), request.getMatchTravelerContext().getTravelerId(),
                ex.getMessage()), ex);
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

    protected String getReqReturnAddr(String lookupRequestId) {
        return reqReturnAddrs.get(lookupRequestId);
    }

    protected DataSourceLookupRequest getReq(String lookupRequestId) {
        return reqs.get(lookupRequestId);
    }

    protected MatchActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public Map<String, Integer> getTotalPendingReqStats() {
        Map<String, Integer> res = new HashMap<>();
        res.put(MatchConstants.REQUEST_NUM, reqs.size());
        res.put(MatchConstants.ADDRESS_NUM, reqReturnAddrs.size());
        ExecutorService executor = actorSystem.getDataSourceServiceExecutor();
        return res;
    }
}
