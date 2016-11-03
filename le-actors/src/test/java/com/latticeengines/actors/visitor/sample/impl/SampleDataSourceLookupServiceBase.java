package com.latticeengines.actors.visitor.sample.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupRequest;
import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchActorSystem;

public abstract class SampleDataSourceLookupServiceBase implements SampleDataSourceLookupService {
    private static final Log log = LogFactory.getLog(SampleDataSourceLookupServiceBase.class);

    @Autowired
    private SampleMatchActorSystem actorSystem;

    abstract protected String lookupFromService(String lookupRequestId, SampleDataSourceLookupRequest request);

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
                if (request instanceof SampleDataSourceLookupRequest) {
                    result = lookupFromService(lookupRequestId, (SampleDataSourceLookupRequest) request);
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
        if (request instanceof SampleDataSourceLookupRequest) {
            String result = lookupFromService(null, (SampleDataSourceLookupRequest) request);
            response.setResult(result);
        }
        return response;
    }

}
