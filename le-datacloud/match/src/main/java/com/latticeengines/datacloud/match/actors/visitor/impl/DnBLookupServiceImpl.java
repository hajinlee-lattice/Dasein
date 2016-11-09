package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.exposed.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Autowired
    private DnBRealTimeLookupService dnbRealTimeLookupService;

    @Autowired
    private DnBBulkLookupDispatcher dnbBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnbBulkLookupFetcher;

    private final ConcurrentMap<String, MatchKeyTuple> reqInputs = new ConcurrentHashMap<String, MatchKeyTuple>();
    private final ConcurrentMap<String, String> reqReturnAddrs = new ConcurrentHashMap<String, String>();
    private final Queue<String> unsubmittedReqIds = new ConcurrentLinkedQueue<String>();
    private final Queue<DnBBulkMatchInfo> submittedReqInfos = new ConcurrentLinkedQueue<DnBBulkMatchInfo>();

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        return dnbRealTimeLookupService.realtimeEntityLookup(matchKeyTuple);
    }

    @Override
    protected void acceptBulkLookup(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        synchronized (reqInputs) {
            reqInputs.put(lookupRequestId, matchKeyTuple);
        }
        synchronized (reqReturnAddrs) {
            reqReturnAddrs.put(lookupRequestId, returnAddress);
        }
        synchronized (unsubmittedReqIds) {
            unsubmittedReqIds.offer(lookupRequestId);
        }
        log.debug("Accepted request " + lookupRequestId);
    }

    @Override
    public void bulkLookup(BulkLookupStrategy timerType) {
        switch (timerType) {
        case DISPATCHER:
            Map<String, MatchKeyTuple> input = new HashMap<String, MatchKeyTuple>();
            synchronized (unsubmittedReqIds) {
                while (!unsubmittedReqIds.isEmpty()) {
                    String lookupRequestId = unsubmittedReqIds.poll();
                    input.put(lookupRequestId, reqInputs.get(lookupRequestId));
                }
            }
            if (!input.isEmpty()) {
                DnBBulkMatchInfo bulkMatchInfo = dnbBulkLookupDispatcher.sendRequest(input);
                if (bulkMatchInfo.getDnbCode() == DnBReturnCode.OK) {
                    synchronized (submittedReqInfos) {
                        submittedReqInfos.offer(bulkMatchInfo);
                    }
                } else {
                    processFailedBulkMatch(bulkMatchInfo);
                }
            }
            break;
        case FETCHER:
            synchronized (submittedReqInfos) {
                DnBBulkMatchInfo submittedReqInfo = submittedReqInfos.peek();
                if (submittedReqInfo != null) {
                    log.debug("Fetching status for batched request " + submittedReqInfo.getServiceBatchId());
                    Map<String, DnBMatchContext> result = dnbBulkLookupFetcher.getResult(submittedReqInfo);
                    if (submittedReqInfo.getDnbCode() == DnBReturnCode.OK) {
                        processSuccessfulBulkMatch(submittedReqInfo, result);
                        submittedReqInfos.poll();
                    } else if (submittedReqInfo.getDnbCode() == DnBReturnCode.EXPIRED
                            || submittedReqInfo.getDnbCode() == DnBReturnCode.INVALID_INPUT
                            || submittedReqInfo.getDnbCode() == DnBReturnCode.TIMEOUT
                            || submittedReqInfo.getDnbCode() == DnBReturnCode.UNKNOWN) {
                        processFailedBulkMatch(submittedReqInfo);
                        submittedReqInfos.poll();
                    } else if (submittedReqInfo.getDnbCode() == DnBReturnCode.EXCEED_CONCURRENT_NUM
                            || submittedReqInfo.getDnbCode() == DnBReturnCode.EXCEED_REQUEST_NUM) {
                        // Should judge if the request is too old
                        processFailedBulkMatch(submittedReqInfo);
                        submittedReqInfos.poll();
                    }
                    log.debug("Status for batched request " + submittedReqInfo.getServiceBatchId() + ": "
                            + submittedReqInfo.getDnbCode().getMessage());
                    /*
                     * If submittedReqInfo.getDnbCode() ==
                     * DnBReturnCode.IN_PROGRESS, do nothing
                     */
                }
            }
            break;
        default:
            break;
        }
        log.debug(submittedReqInfos.size()
                + " batched requests have been submitted to DnB bulk match api and waiting for results");
    }

    private void processFailedBulkMatch(DnBBulkMatchInfo bulkMatchInfo) {
        for (String lookupRequestId : bulkMatchInfo.getLookupRequestIds()) {
            DnBMatchContext result = new DnBMatchContext();
            result.setInputNameLocation(reqInputs.get(lookupRequestId));
            result.setDnbCode(bulkMatchInfo.getDnbCode());
            sendResponse(lookupRequestId, result, reqReturnAddrs.get(lookupRequestId));
            reqInputs.remove(lookupRequestId);
            reqReturnAddrs.remove(lookupRequestId);
        }
    }

    private void processSuccessfulBulkMatch(DnBBulkMatchInfo bulkMatchInfo, Map<String, DnBMatchContext> output) {
        for (String lookupRequestId : bulkMatchInfo.getLookupRequestIds()) {
            if (output.containsKey(lookupRequestId)) {
                DnBMatchContext result = output.get(lookupRequestId);
                result.setInputNameLocation(reqInputs.get(lookupRequestId));
                sendResponse(lookupRequestId, result, reqReturnAddrs.get(lookupRequestId));
            } else {
                DnBMatchContext result = new DnBMatchContext();
                result.setInputNameLocation(reqInputs.get(lookupRequestId));
                result.setDnbCode(DnBReturnCode.NO_RESULT);
                sendResponse(lookupRequestId, result, reqReturnAddrs.get(lookupRequestId));
            }
            synchronized (reqInputs) {
                reqInputs.remove(lookupRequestId);
            }
            synchronized (reqReturnAddrs) {
                reqReturnAddrs.remove(lookupRequestId);
            }
        }
    }

    private void sendResponse(String lookupRequestId, DnBMatchContext result, String returnAddress) {
        Response response = new Response();
        response.setRequestId(lookupRequestId);
        response.setResult(result);
        log.debug("Returned response for " + lookupRequestId + " to " + returnAddress);
        actorSystem.sendResponse(response, returnAddress);
    }
}
