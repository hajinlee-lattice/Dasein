//package com.latticeengines.actors.visitor.sample.impl;
//
//import java.util.Map;
//import java.util.UUID;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.springframework.stereotype.Component;
//
//import com.latticeengines.actors.exposed.traveler.Response;
//import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
//import com.latticeengines.actors.visitor.sample.SampleMatchActorSystemWrapper;
//
//@Component("SampleDynamoDBLookupService")
//public class SampleDynamoDBLookupServiceImpl implements SampleDataSourceLookupService {
//    private static final Log log = LogFactory.getLog(SampleDynamoDBLookupServiceImpl.class);
//
//    @Override
//    public void asyncLookup(String lookupId, Object inputData, String returnAddress, Object system) {
//        // do async processing
//        log.info("Doing async lookup");
//
//        Thread th = new Thread(createLookupRunnable(lookupId, inputData, returnAddress, system));
//        th.start();
//    }
//
//    private Runnable createLookupRunnable(final String lookupId, final Object inputData, final String returnAddress,
//            final Object system) {
//        // sample impl
//        Runnable task = new Runnable() {
//
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(3000);
//                } catch (InterruptedException e) {
//                }
//
//                Object result = null;
//
//                @SuppressWarnings("unchecked")
//                Map<String, Object> dataMap = (Map) inputData;
//                if (dataMap.containsKey("DUNS") && dataMap.containsKey("Domain")) {
//                    log.info("Got result for " + lookupId + " from DynamoDB");
//
//                    result = "LatticeAccountID_" + UUID.randomUUID().toString();
//
//                } else {
//                    log.info("Didn't find result for " + lookupId + " from DynamoDB");
//                }
//
//                Response response = new Response();
//                response.setRequestId(lookupId);
//                response.setResult(result);
//
//                log.info("Returned response for " + lookupId + " to " + returnAddress);
//                SampleMatchActorSystemWrapper.sendResponse(system, response, returnAddress);
//            }
//        };
//        return task;
//    }
//
//    @Override
//    public Response syncLookup(Object inputData) {
//        Object result = UUID.randomUUID().toString();
//        Response response = new Response();
//        response.setResult(result);
//        log.info("Got result from lookup");
//        return response;
//    }
//}
