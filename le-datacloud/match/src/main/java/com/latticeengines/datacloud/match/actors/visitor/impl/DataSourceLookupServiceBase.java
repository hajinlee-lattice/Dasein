package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.MatchMetricService;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;

public abstract class DataSourceLookupServiceBase implements DataSourceLookupService {
    private static final Logger log = LoggerFactory.getLogger(DataSourceLookupServiceBase.class);

    @Lazy
    @Inject
    protected MatchMetricService matchMetricService;

    @Autowired
    private MatchActorSystem actorSystem;

    private final ConcurrentMap<String, String> reqReturnAddrs = new ConcurrentHashMap<String, String>();

    private final ConcurrentMap<String, DataSourceLookupRequest> reqs = new ConcurrentHashMap<>();

    private volatile boolean instantiated = false;

    protected abstract Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request);

    protected abstract void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress);

    @Override
    public void asyncLookup(String lookupRequestId, Object request, String returnAddress) {
        registerForMonitoring();
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
        registerForMonitoring();
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
        MatchTraveler travaler = request.getMatchTravelerContext();
        String errorMsg = String.format("Encountered issue at %s for request %s%s: %s",
                this.getClass().getSimpleName(), request.getMatchTravelerContext().getTravelerId(),
                request.getMatchTravelerContext().getMatchInput().getRootOperationUid() == null ? ""
                                : " (RootOperationID="
                                        + request.getMatchTravelerContext().getMatchInput().getRootOperationUid() + ")",
                ex.getMessage());
        log.error(errorMsg, ex);
        travaler.error(errorMsg, ex);
        actorSystem.sendResponse(travaler, request.getMatchTravelerContext().getOriginalLocation());
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

    // Only for testing purpose
    protected void injectFailure(DataSourceLookupRequest req) {
        if (req == null || req.getMatchTravelerContext() == null) {
            return;
        }
        MatchTraveler traveler = req.getMatchTravelerContext();
        if (this.getClass().getSimpleName().equals(traveler.getActorOrServiceToInjectFailure())) {
            throw new RuntimeException(ActorUtils.INJECTED_FAILURE_MSG);
        }
    }

    /**
     * Whether to monitor stats of this service.
     *
     * @return flag to enable/disable monitoring
     */
    protected boolean enableMonitoring() {
        return false;
    }

    @Override
    public Map<String, Integer> getTotalPendingReqStats() {
        Map<String, Integer> res = new HashMap<>();
        res.put(MatchConstants.REQUEST_NUM, reqs.size());
        res.put(MatchConstants.ADDRESS_NUM, reqReturnAddrs.size());
        ThreadPoolExecutor executor = (ThreadPoolExecutor) actorSystem.getDataSourceServiceExecutor();
        res.put(MatchConstants.ACTIVE_REQ_NUM, executor == null ? 0 : executor.getActiveCount());
        res.put(MatchConstants.QUEUED_REQ_NUM,
                executor == null || executor.getQueue() == null ? 0 : executor.getQueue().size());
        return res;
    }

    /*
     * register self for monitoring after the first request
     */
    private void registerForMonitoring() {
        if (instantiated) {
            return;
        }

        synchronized (this) {
            if (!instantiated) {
                if (enableMonitoring()) {
                    matchMetricService.registerDataSourceLookupService(this, actorSystem.isBatchMode());
                }
                instantiated = true;
            }
        }
    }
}
