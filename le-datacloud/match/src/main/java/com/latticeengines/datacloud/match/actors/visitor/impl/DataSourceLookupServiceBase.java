package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;

public abstract class DataSourceLookupServiceBase implements DataSourceLookupService {
    private static final Log log = LogFactory.getLog(DataSourceLookupServiceBase.class);

    @Autowired
    private MatchActorSystem actorSystem;

    abstract protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request);

    abstract protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress);

    @Override
    public void asyncLookup(String lookupRequestId, Object request, String returnAddress) {
        ThreadPoolTaskExecutor executor = actorSystem.getDataSourceServiceExecutor();
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

    protected boolean isBatchMode() {
        return actorSystem.isBatchMode();
    }

}
