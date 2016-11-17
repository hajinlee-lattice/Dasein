package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;

@Component("dynamoDBLookupService")
public class DynamoDBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DynamoDBLookupServiceImpl.class);

    @Autowired
    private AccountLookupService accountLookupService;

    private ExecutorService executor;

    @Value("${datacloud.match.dynamo.fetchers.num:16}")
    private Integer fetcherNum;

    @Value("${datacloud.match.num.dynamo.fetchers.batch.num:4}")
    private Integer batchFetcherNum;

    @Value("${datacloud.match.dynamo.fetchers.size:25}")
    private Integer fetcherSize;

    private boolean fetchersInitiated = false;

    private boolean enableFetchers = false;

    private final ConcurrentMap<String, String> reqReturnAddrs = new ConcurrentHashMap<String, String>();
    private final ConcurrentMap<String, DataSourceLookupRequest> reqs = new ConcurrentHashMap<String, DataSourceLookupRequest>();
    private final Queue<String> pendingReqIds = new ConcurrentLinkedQueue<String>();

    @PostConstruct
    private void postConstruct() {
        initExecutors();
    }

    public void initExecutors() {
        if (fetchersInitiated) {
            // do nothing if fetcher executors are already started
            return;
        }

        log.info("Initialize dynamo fetcher executors.");
        Integer num = isBatchMode() ? batchFetcherNum : fetcherNum;
        executor = Executors.newFixedThreadPool(num);

        for (int i = 0; i < num; i++) {
            executor.submit(new Fetcher());
        }

        fetchersInitiated = true;
        enableFetchers = true;
    }


    @Override
    protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        String result = null;
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();

        if (matchKeyTuple.getDuns() != null || matchKeyTuple.getDomain() != null) {
            AccountLookupRequest accountLookupRequest = new AccountLookupRequest(
                    request.getMatchTravelerContext().getDataCloudVersion());
            accountLookupRequest.addLookupPair(matchKeyTuple.getDomain(), matchKeyTuple.getDuns());
            Long startTime = System.currentTimeMillis();
            result = accountLookupService.batchLookupIds(accountLookupRequest).get(0);
            log.info(String.format("Fetched results from Dynamo for 1 sync requests (DataCloudVersion=%s) Duration=%d",
                    request.getMatchTravelerContext().getDataCloudVersion(), System.currentTimeMillis() - startTime));
            if (StringUtils.isNotEmpty(result)) {
                if (log.isDebugEnabled()) {
                    log.debug("Got result from lookup for Lookup key=" + accountLookupRequest.getIds().get(0)
                            + " Lattice Account Id=" + result);
                }
            } else {
                // may not be able to handle empty string
                result = null;
                if (log.isDebugEnabled()) {
                    log.debug("Didn't get anything from dynamodb for " + lookupRequestId);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Skip lookup into dynamodb for " + lookupRequestId);
            }
        }

        return result;
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        if (matchKeyTuple.getDuns() == null && matchKeyTuple.getDomain() == null) {
            if (log.isDebugEnabled()) {
                log.debug("Skip lookup into dynamodb for " + lookupRequestId);
            }
            sendResponse(lookupRequestId, null, returnAddress);
            return;
        }

        if (!enableFetchers) {
            if (log.isDebugEnabled()) {
                log.debug("Fetcher is not enabled. Skip bucketing.");
            }
            String result = lookupFromService(lookupRequestId, request);
            sendResponse(lookupRequestId, result, returnAddress);
            return;
        }

        reqReturnAddrs.put(lookupRequestId, returnAddress);
        reqs.put(lookupRequestId, request);
        pendingReqIds.offer(lookupRequestId);
        synchronized (pendingReqIds) {
            if (pendingReqIds.size() > 0 && pendingReqIds.size() <= fetcherSize) {
                pendingReqIds.notify();
            } else if (pendingReqIds.size() > fetcherSize) {
                pendingReqIds.notifyAll();
            }
        }
    }

    private class Fetcher implements Runnable {
        @Override
        public void run() {
            while(true) {
                Map<String, List<String>> reqIdsWithVersion = new HashMap<String, List<String>>();
                Map<String, AccountLookupRequest> lookupReqWithVersion = new HashMap<String, AccountLookupRequest>();
                synchronized (pendingReqIds) {
                    while (pendingReqIds.isEmpty()) {
                        try {
                            pendingReqIds.wait();
                        } catch (InterruptedException e) {
                            log.error(e);
                        }
                    }
                    for (int i = 0; i < Math.min(pendingReqIds.size(), fetcherSize); i++) {
                        String lookupRequestId = pendingReqIds.poll();
                        DataSourceLookupRequest req = reqs.get(lookupRequestId);
                        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) req.getInputData();
                        String dataCloudVersion = req.getMatchTravelerContext().getDataCloudVersion();
                        if (!lookupReqWithVersion.containsKey(dataCloudVersion)) {
                            lookupReqWithVersion.put(dataCloudVersion, new AccountLookupRequest(dataCloudVersion));
                        }
                        if (!reqIdsWithVersion.containsKey(dataCloudVersion)) {
                            reqIdsWithVersion.put(dataCloudVersion, new ArrayList<String>());
                        }
                        lookupReqWithVersion.get(dataCloudVersion).addLookupPair(matchKeyTuple.getDomain(),
                                matchKeyTuple.getDuns());
                        reqIdsWithVersion.get(dataCloudVersion).add(lookupRequestId);
                    }
                }
                for (String dataCloudVersion : lookupReqWithVersion.keySet()) {
                    Long startTime = System.currentTimeMillis();
                    List<String> results = accountLookupService
                            .batchLookupIds(lookupReqWithVersion.get(dataCloudVersion));
                    List<String> reqIds = reqIdsWithVersion.get(dataCloudVersion);
                    log.info(String.format(
                            "Fetched results from Dynamo for %d async requests (DataCloudVersion=%s) Duration=%d",
                            reqIds.size(), dataCloudVersion, System.currentTimeMillis() - startTime));
                    if (results.size() != reqIds.size()) {
                        log.error("Dynamo lookup failed to return complete matching results");
                    }
                    for (int i = 0; i < Math.min(results.size(), reqIdsWithVersion.get(dataCloudVersion).size()); i++) {
                        String result = results.get(i);
                        if (StringUtils.isNotEmpty(result)) {
                            if (log.isDebugEnabled()) {
                                log.debug("Got result from lookup for Lookup key=" + reqIds.get(i)
                                        + " Lattice Account Id=" + result);
                            }
                        } else {
                            // may not be able to handle empty string
                            result = null;
                            if (log.isDebugEnabled()) {
                                log.debug("Didn't get anything from dynamodb for " + reqIds.get(i));
                            }
                        }
                        sendResponse(reqIds.get(i), result, reqReturnAddrs.get(reqIds.get(i)));
                        reqReturnAddrs.remove(reqIds.get(i));
                        reqs.remove(reqIds.get(i));
                    }
                }
            }
        }
    }

    @Override
    public void bulkLookup(BulkLookupStrategy bulkLookupStrategy) {
    }


}
