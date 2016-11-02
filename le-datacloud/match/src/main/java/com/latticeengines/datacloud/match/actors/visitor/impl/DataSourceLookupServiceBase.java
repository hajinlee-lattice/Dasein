package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;

public abstract class DataSourceLookupServiceBase implements DataSourceLookupService {
    private static final Log log = LogFactory.getLog(DataSourceLookupServiceBase.class);

    @Autowired
    private MatchActorSystem actorSystem;

    abstract protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request);

    @Override
    public void asyncLookup(String lookupRequestId, Object request, String returnAddress) {
        log.info("Doing async lookup");
        Thread th = new Thread(createLookupRunnable(lookupRequestId, request, returnAddress));
        th.start();
    }

    private Runnable createLookupRunnable(final String lookupRequestId, final Object request,
            final String returnAddress) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }

                String result = null;
                if (request instanceof DataSourceLookupRequest) {
                    result = lookupFromService(lookupRequestId, (DataSourceLookupRequest) request);
                }

                Response response = new Response();
                response.setRequestId(lookupRequestId);
                response.setResult(result);

                log.info("Returned response for " + lookupRequestId + " to " + returnAddress);
                actorSystem.sendResponse(response, returnAddress);
            }
        };
        return task;
    }

    @Override
    public Response syncLookup(Object request) {
        Response response = new Response();
        if (request instanceof DataSourceLookupRequest) {
            String result = lookupFromService(null, (DataSourceLookupRequest) request);
            response.setResult(result);
        }
        return response;
    }

}
