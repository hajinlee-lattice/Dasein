package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBBlackCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Value("${datacloud.dnb.bulk.request.expire.duration.minute}")
    private int requestExpireDuration;

    @Autowired
    private DnBRealTimeLookupService dnbRealTimeLookupService;

    @Autowired
    private DnBBulkLookupDispatcher dnbBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnbBulkLookupFetcher;

    @Autowired
    private DnBCacheService dnbCacheService;

    private final Queue<DnBMatchContext> unsubmittedReqs = new ConcurrentLinkedQueue<DnBMatchContext>();
    private final Queue<DnBBatchMatchContext> submittedReqs = new ConcurrentLinkedQueue<DnBBatchMatchContext>();
    private static AtomicInteger unsubmittedNum = new AtomicInteger(0);

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext context = new DnBMatchContext();
        context.setLookupRequestId(lookupRequestId);
        context.setInputNameLocation(matchKeyTuple);
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        DnBWhiteCache whiteCache = dnbCacheService.lookupWhiteCache(context);
        if (whiteCache != null) {
            context.copyResultFromWhiteCache(whiteCache);
            return context;
        }
        DnBBlackCache blackCache = dnbCacheService.lookupBlackCache(context);
        if (blackCache != null) {
            context.copyResultFromBlackCache(blackCache);
            return context;
        }
        context = dnbRealTimeLookupService.realtimeEntityLookup(context);
        dnbCacheService.addCache(context);
        List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
        dnBMatchHistories.add(new DnBMatchHistory(context));
        writeDnBMatchHistory(dnBMatchHistories);
        return context;
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

    protected void acceptBulkLookup(String lookupRequestId, DataSourceLookupRequest request, String returnAddress) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext context = new DnBMatchContext();
        context.setLookupRequestId(lookupRequestId);
        context.setInputNameLocation(matchKeyTuple);
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
        DnBWhiteCache whiteCache = dnbCacheService.lookupWhiteCache(context);
        if (whiteCache != null) {
            context.copyResultFromWhiteCache(whiteCache);
            sendResponse(lookupRequestId, context, returnAddress);
            return;
        }
        DnBBlackCache blackCache = dnbCacheService.lookupBlackCache(context);
        if (blackCache != null) {
            context.copyResultFromBlackCache(blackCache);
            sendResponse(lookupRequestId, context, returnAddress);
            return;
        }

        saveReq(lookupRequestId, returnAddress, request);
        unsubmittedReqs.offer(context);

        if (log.isDebugEnabled()) {
            log.debug("Accepted request " + context.getLookupRequestId());
        }
    }

    @Override
    public void bulkLookup(BulkLookupStrategy bulkLookupStrategy) {
        try {
        switch (bulkLookupStrategy) {
        case DISPATCHER:
            List<DnBBatchMatchContext> batchContexts = new ArrayList<DnBBatchMatchContext>();
            synchronized (unsubmittedReqs) {
                if (unsubmittedReqs.size() >= 10000
                        || (!unsubmittedReqs.isEmpty() && unsubmittedReqs.size() == unsubmittedNum.get())) {
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
                unsubmittedNum.set(unsubmittedReqs.size());
            }
            if (!batchContexts.isEmpty()) {
                for (DnBBatchMatchContext batchContext : batchContexts) {
                    try {
                        batchContext = dnbBulkLookupDispatcher.sendRequest(batchContext);
                    } catch (Exception ex) {
                        log.error(String.format("Exception in dispatching match requests to DnB bulk match service: %s",
                                ex.getMessage()));
                        batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
                    }
                    if (batchContext.getDnbCode() == DnBReturnCode.OK) {
                        submittedReqs.offer(batchContext);
                    } else {
                        processBulkMatchResult(batchContext, false);
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
                    try {
                        submittedReq = dnbBulkLookupFetcher.getResult(submittedReq);
                    } catch (Exception ex) {
                        log.error(String.format(
                                "Fail to poll match result for request %s from DnB bulk matchc service: %s",
                                submittedReq.getServiceBatchId(), ex.getMessage()));
                        submittedReq.setDnbCode(DnBReturnCode.UNKNOWN);
                    }
                    if (submittedReq.getDnbCode() == DnBReturnCode.OK) {
                        processBulkMatchResult(submittedReq, true);
                        submittedReqs.poll();
                    } else if (submittedReq.getDnbCode() == DnBReturnCode.IN_PROGRESS
                            || submittedReq.getDnbCode() == DnBReturnCode.RATE_LIMITING) {
                        // Do nothing
                    } else if (submittedReq.getDnbCode() == DnBReturnCode.EXCEED_CONCURRENT_NUM
                            || submittedReq.getDnbCode() == DnBReturnCode.EXCEED_REQUEST_NUM) {
                        Date now = new Date();
                        if (submittedReq.getTimestamp() == null
                                || (now.getTime() - submittedReq.getTimestamp().getTime()) / 1000
                                        / 60 > requestExpireDuration) {
                            processBulkMatchResult(submittedReq, false);
                            submittedReqs.poll();
                        }
                    } else {
                        processBulkMatchResult(submittedReq, false);
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
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private void processBulkMatchResult(DnBBatchMatchContext batchContext, boolean success) {
        List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
        for (String lookupRequestId : batchContext.getContexts().keySet()) {
            DnBMatchContext context = batchContext.getContexts().get(lookupRequestId);
            if (!success) {
                context.setDnbCode(batchContext.getDnbCode());
            } else {
                dnbCacheService.addCache(context);
            }
            sendResponse(lookupRequestId, context, getReqReturnAdd(lookupRequestId));
            removeReq(lookupRequestId);
            dnBMatchHistories.add(new DnBMatchHistory(context));
        }
        writeDnBMatchHistory(dnBMatchHistories);
    }

    private void writeDnBMatchHistory(List<DnBMatchHistory> metrics) {
        try {
            MeasurementMessage<DnBMatchHistory> message = new MeasurementMessage<>();
            message.setMeasurements(metrics);
            message.setMetricDB(MetricDB.LDC_Match);
            getActorSystem().getMetricActor().tell(message, null);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }

}
