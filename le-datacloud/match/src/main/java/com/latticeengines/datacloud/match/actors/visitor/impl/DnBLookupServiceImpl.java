package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Autowired
    private DnBRealTimeLookupService dnbRealTimeLookupService;

    @Autowired
    private DnBBulkLookupDispatcher dnbBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnbBulkLookupFetcher;

    private final ConcurrentMap<String, String> reqReturnAddrs = new ConcurrentHashMap<String, String>();
    private final Queue<DnBMatchContext> unsubmittedReqs = new ConcurrentLinkedQueue<DnBMatchContext>();
    private final Queue<DnBBatchMatchContext> submittedReqs = new ConcurrentLinkedQueue<DnBBatchMatchContext>();

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext matchContext = new DnBMatchContext();
        matchContext.setLookupRequestId(lookupRequestId);
        matchContext.setInputNameLocation(matchKeyTuple);
        return dnbRealTimeLookupService.realtimeEntityLookup(matchContext);
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        if (!isBatchMode()) {
            DnBMatchContext result = (DnBMatchContext) lookupFromService(lookupRequestId,
                    (DataSourceLookupRequest) request);
            sendResponse(lookupRequestId, result, returnAddress);
        } else {
            acceptBulkLookup(lookupRequestId, (DataSourceLookupRequest) request, returnAddress);
        }
    }

    protected void acceptBulkLookup(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext matchContext = new DnBMatchContext();
        matchContext.setLookupRequestId(lookupRequestId);
        matchContext.setInputNameLocation(matchKeyTuple);
        synchronized (reqReturnAddrs) {
            reqReturnAddrs.put(lookupRequestId, returnAddress);
        }
        synchronized (unsubmittedReqs) {
            unsubmittedReqs.offer(matchContext);
        }
        if (log.isDebugEnabled()) {
            log.debug("Accepted request " + matchContext.getLookupRequestId());
        }
    }

    @Override
    public void bulkLookup(BulkLookupStrategy bulkLookupStrategy) {
        switch (bulkLookupStrategy) {
        case DISPATCHER:
            List<DnBBatchMatchContext> batchContexts = new ArrayList<DnBBatchMatchContext>();
            synchronized (unsubmittedReqs) {
                if (!unsubmittedReqs.isEmpty()) {
                    int num = (int) Math.ceil(unsubmittedReqs.size() / 10000.0);
                    int batchSize = (int) Math.ceil(unsubmittedReqs.size() / num);
                    for (int i = 0; i < num; i++) {
                        DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
                        for (int j = 0; j < batchSize; j++) {
                            DnBMatchContext context = unsubmittedReqs.poll();
                            if (context == null) {
                                break;
                            }
                            batchContext.getContexts().put(context.getLookupRequestId(), context);
                        }
                        if (!batchContext.getContexts().isEmpty()) {
                            batchContexts.add(batchContext);
                        } else {
                            break;
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Is unsubmittedReqs empty? " + unsubmittedReqs.isEmpty());
                    }
                }
            }
            if (!batchContexts.isEmpty()) {
                for (DnBBatchMatchContext batchContext : batchContexts) {
                    batchContext = dnbBulkLookupDispatcher.sendRequest(batchContext);
                    if (batchContext.getDnbCode() == DnBReturnCode.OK) {
                        synchronized (submittedReqs) {
                            submittedReqs.offer(batchContext);
                        }
                    } else {
                        processMatchResult(batchContext, false);
                    }
                }


            }
            break;
        case FETCHER:
            synchronized (submittedReqs) {
                DnBBatchMatchContext submittedReq = submittedReqs.peek();
                if (submittedReq != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Fetching status for batched request " + submittedReq.getServiceBatchId());
                    }
                    submittedReq = dnbBulkLookupFetcher.getResult(submittedReq);
                    if (submittedReq.getDnbCode() == DnBReturnCode.OK) {
                        processMatchResult(submittedReq, true);
                        submittedReqs.poll();
                    } else if (submittedReq.getDnbCode() == DnBReturnCode.EXPIRED
                            || submittedReq.getDnbCode() == DnBReturnCode.BAD_REQUEST
                            || submittedReq.getDnbCode() == DnBReturnCode.TIMEOUT
                            || submittedReq.getDnbCode() == DnBReturnCode.UNKNOWN) {
                        processMatchResult(submittedReq, false);
                        submittedReqs.poll();
                    } else if (submittedReq.getDnbCode() == DnBReturnCode.EXCEED_CONCURRENT_NUM
                            || submittedReq.getDnbCode() == DnBReturnCode.EXCEED_REQUEST_NUM) {
                        // Should judge if the request is too old
                        processMatchResult(submittedReq, false);
                        submittedReqs.poll();
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Status for batched request " + submittedReq.getServiceBatchId() + ": "
                                + submittedReq.getDnbCode().getMessage());
                    }
                }
            }
            break;
        default:
            throw new UnsupportedOperationException(
                    "BulkLookupStrategy " + bulkLookupStrategy.name() + " is not supported in DnB bulk match");
        }
        if (log.isDebugEnabled()) {
            log.debug(submittedReqs.size() + " batched requests are waiting for DnB batch api to return results");
        }
    }

    private void processMatchResult(DnBBatchMatchContext batchContext, boolean success) {
        for (String lookupRequestId : batchContext.getContexts().keySet()) {
            DnBMatchContext context = batchContext.getContexts().get(lookupRequestId);
            if (!success) {
                context.setDnbCode(batchContext.getDnbCode());
            }
            sendResponse(lookupRequestId, context, reqReturnAddrs.get(lookupRequestId));
            synchronized (reqReturnAddrs) {
                reqReturnAddrs.remove(lookupRequestId);
            }

        }
    }


}
