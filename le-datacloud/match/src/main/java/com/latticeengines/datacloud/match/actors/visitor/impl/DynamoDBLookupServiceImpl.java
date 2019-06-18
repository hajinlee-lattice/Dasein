package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TERMINATE_EXECUTOR_TIMEOUT_MS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DynamoDBLookupService;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("dynamoDBLookupService")
public class DynamoDBLookupServiceImpl extends DataSourceLookupServiceBase implements DynamoDBLookupService {
    private static final Logger log = LoggerFactory.getLogger(DynamoDBLookupServiceImpl.class);

    @Autowired
    private AccountLookupService accountLookupService;

    @Value("${datacloud.match.dynamo.fetchers.num}")
    private Integer fetcherNum;

    @Value("${datacloud.match.num.dynamo.fetchers.batch.num}")
    private Integer batchFetcherNum;

    @Value("${datacloud.match.dynamo.fetchers.chunk.size}")
    private Integer chunkSize;

    private final AtomicBoolean fetchersInitiated = new AtomicBoolean(false);

    private final Queue<String> pendingReqIds = new ConcurrentLinkedQueue<>();

    private ExecutorService dynamoFetcher;

    // flag to indicate whether background fetcher should keep running
    private volatile boolean shouldTerminate = false;

    private void initExecutors() {
        synchronized (fetchersInitiated) {
            if (fetchersInitiated.get()) {
                // do nothing if fetcher executors are already started
                return;
            }

            log.info("Initialize dynamo fetcher executors.");
            Integer num = isBatchMode() ? batchFetcherNum : fetcherNum;
            dynamoFetcher = ThreadPoolUtils.getFixedSizeThreadPool("dynamo-fetcher", num);

            for (int i = 0; i < num; i++) {
                dynamoFetcher.submit(new Fetcher());
            }

            fetchersInitiated.set(true);
        }
    }

    @PreDestroy
    private void preDestroy() {
        try {
            if (shouldTerminate) {
                return;
            }
            log.info("Shutting down Dynamo fetchers");
            shouldTerminate = true;
            if (dynamoFetcher != null) {
                dynamoFetcher.shutdownNow();
                dynamoFetcher.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            log.info("Completed shutting down of Dynamo fetchers");
        } catch (Exception e) {
            log.error("Fail to finish all pre-destroy actions", e);
        }
    }


    @Override
    protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        String latticeAccountId = null;
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();

        if (matchKeyTuple.getDuns() != null || matchKeyTuple.getDomain() != null) {
            AccountLookupRequest accountLookupRequest = new AccountLookupRequest(
                    request.getMatchTravelerContext().getDataCloudVersion());
            if (request.getCallerMicroEngineReference() != null
                    && (StringUtils.contains(request.getCallerMicroEngineReference(),
                            DomainCountryBasedMicroEngineActor.class.getSimpleName())
                    || StringUtils.contains(request.getCallerMicroEngineReference(),
                            DomainCountryZipCodeBasedMicroEngineActor.class.getSimpleName())
                    || StringUtils.contains(request.getCallerMicroEngineReference(),
                            DomainCountryStateBasedMicroEngineActor.class.getSimpleName()))) {
                accountLookupRequest.addLookupPair(matchKeyTuple.getDomain(), matchKeyTuple.getDuns(),
                        matchKeyTuple.getCountry(), matchKeyTuple.getState(), matchKeyTuple.getZipcode());
            } else {
                accountLookupRequest.addLookupPair(matchKeyTuple.getDomain(), matchKeyTuple.getDuns());
            }
            Long startTime = System.currentTimeMillis();
            AccountLookupEntry lookupEntry = accountLookupService.batchLookup(accountLookupRequest).get(0);
            latticeAccountId = (lookupEntry == null) ? null : lookupEntry.getLatticeAccountId();
            log.info(String.format(
                    "Fetched result from Dynamo for 1 sync requests (DataCloudVersion=%s) Duration=%d Result is %sempty",
                    request.getMatchTravelerContext().getDataCloudVersion(), System.currentTimeMillis() - startTime,
                    latticeAccountId == null ? "" : "not "));
            if (StringUtils.isNotEmpty(latticeAccountId)) {
                if (log.isDebugEnabled()) {
                    log.debug("Got result from lookup for Lookup key=" + accountLookupRequest.getIds().get(0)
                            + " Lattice Account Id=" + latticeAccountId);
                }
                // Put duns for matched ldc account into traveler for entity
                // match
                request.getMatchTravelerContext().setDunsOriginMapIfAbsent(new HashMap<>());
                request.getMatchTravelerContext().getDunsOriginMap().put(DataCloudConstants.ACCOUNT_MASTER,
                        lookupEntry.getLdcDuns());
            } else {
                // may not be able to handle empty string
                latticeAccountId = null;
                if (log.isDebugEnabled()) {
                    log.debug("Didn't get anything from dynamodb for " + lookupRequestId);
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Skip lookup into dynamodb for " + lookupRequestId);
            }
        }

        return latticeAccountId;
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

        if (!fetchersInitiated.get()) {
            initExecutors();
        }

        saveReq(lookupRequestId, returnAddress, request);
        pendingReqIds.offer(lookupRequestId);
        synchronized (pendingReqIds) {
            if (pendingReqIds.size() > 0 && pendingReqIds.size() <= chunkSize) {
                pendingReqIds.notify();
            } else if (pendingReqIds.size() > chunkSize) {
                pendingReqIds.notifyAll();
            }
        }
    }

    private class Fetcher implements Runnable {
        @Override
        public void run() {
            while (!shouldTerminate) {
                // DataCloudVersion -> ReqIds
                Map<String, List<String>> reqIdsWithVersion = new HashMap<>();
                Map<String, AccountLookupRequest> lookupReqWithVersion = new HashMap<String, AccountLookupRequest>();
                synchronized (pendingReqIds) {
                    while (!shouldTerminate && pendingReqIds.isEmpty()) {
                        try {
                            pendingReqIds.wait();
                        } catch (InterruptedException e) {
                            if (!shouldTerminate) {
                                log.warn("Dynamo fetcher (in background) is interrupted");
                            }
                        }
                    }
                    for (int i = 0; i < Math.min(pendingReqIds.size(), chunkSize); i++) {
                        String lookupRequestId = pendingReqIds.poll();
                        DataSourceLookupRequest req = getReq(lookupRequestId);
                        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) req.getInputData();
                        String dataCloudVersion = req.getMatchTravelerContext().getDataCloudVersion();
                        if (!lookupReqWithVersion.containsKey(dataCloudVersion)) {
                            lookupReqWithVersion.put(dataCloudVersion, new AccountLookupRequest(dataCloudVersion));
                        }
                        if (!reqIdsWithVersion.containsKey(dataCloudVersion)) {
                            reqIdsWithVersion.put(dataCloudVersion, new ArrayList<>());
                        }
                        if (req.getCallerMicroEngineReference() != null
                                && (StringUtils.contains(req.getCallerMicroEngineReference(),
                                        DomainCountryBasedMicroEngineActor.class.getSimpleName())
                                || StringUtils.contains(req.getCallerMicroEngineReference(),
                                        DomainCountryZipCodeBasedMicroEngineActor.class.getSimpleName())
                                || StringUtils.contains(req.getCallerMicroEngineReference(),
                                        DomainCountryStateBasedMicroEngineActor.class.getSimpleName()))) {
                            lookupReqWithVersion.get(dataCloudVersion).addLookupPair(matchKeyTuple.getDomain(),
                                    matchKeyTuple.getDuns(), matchKeyTuple.getCountry(), matchKeyTuple.getState(),
                                    matchKeyTuple.getZipcode());
                        } else {
                            lookupReqWithVersion.get(dataCloudVersion).addLookupPair(matchKeyTuple.getDomain(),
                                    matchKeyTuple.getDuns());
                        }

                        reqIdsWithVersion.get(dataCloudVersion).add(lookupRequestId);
                    }
                }
                for (String dataCloudVersion : lookupReqWithVersion.keySet()) {
                    try {
                        List<AccountLookupEntry> lookupEntries = accountLookupService
                                .batchLookup(lookupReqWithVersion.get(dataCloudVersion));
                        List<String> reqIds = reqIdsWithVersion.get(dataCloudVersion);
                        if (lookupEntries == null) {
                            throw new RuntimeException(String.format(
                                    "Dynamo lookup got null matching results: submitted %d requests", reqIds.size()));
                        }
                        if (lookupEntries.size() != reqIds.size()) {
                            throw new RuntimeException(String.format(
                                    "Dynamo lookup failed to return complete matching results: submitted %d requests but only got %d results back",
                                    reqIds.size(), lookupEntries.size()));
                        }
                        for (int i = 0; i < lookupEntries.size(); i++) {
                            AccountLookupEntry lookupEntry = lookupEntries.get(i);
                            if (lookupEntry != null && StringUtils.isNotEmpty(lookupEntry.getLatticeAccountId())) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Got result from lookup for Lookup key=" + reqIds.get(i)
                                            + " Lattice Account Id=" + lookupEntry);
                                }
                                // Put duns for matched ldc account into
                                // traveler for entity match
                                DataSourceLookupRequest req = getReq(reqIds.get(i));
                                req.getMatchTravelerContext().setDunsOriginMapIfAbsent(new HashMap<>());
                                req.getMatchTravelerContext().getDunsOriginMap().put(DataCloudConstants.ACCOUNT_MASTER,
                                        lookupEntry.getLdcDuns());
                            } else {
                                // may not be able to handle empty string
                                lookupEntry = null;
                                if (log.isDebugEnabled()) {
                                    log.debug("Didn't get anything from dynamodb for " + reqIds.get(i));
                                }
                            }
                            // Inject failure only for testing purpose
                            injectFailure(getReq(reqIds.get(i)));
                            String returnAddr = getReqReturnAddr(reqIds.get(i));
                            removeReq(reqIds.get(i));
                            sendResponse(reqIds.get(i), lookupEntry, returnAddr);
                        }
                    } catch (Exception ex) {
                        log.error("Fail to lookup in Dynamo table for datacloud version " + dataCloudVersion, ex);
                        List<String> reqIds = reqIdsWithVersion.get(dataCloudVersion);
                        for (String reqId : reqIds) {
                            DataSourceLookupRequest req = getReq(reqId);
                            if (req != null) {
                                removeReq(reqId);
                                sendFailureResponse(req, ex);
                            }
                        }
                    }
                }
            }
        }
    }
    
    /************************ Dynamo Lookup Service Status ************************/
    @Override
    public Map<String, Integer> getPendingReqStats() {
        Map<String, Integer> res = new HashMap<>();
        res.put(MatchConstants.REQUEST_NUM, pendingReqIds.size());
        return res;
    }
}
