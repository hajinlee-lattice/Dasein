package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private void initExecutors() {
        synchronized (fetchersInitiated) {
            if (fetchersInitiated.get()) {
                // do nothing if fetcher executors are already started
                return;
            }

            log.info("Initialize dynamo fetcher executors.");
            Integer num = isBatchMode() ? batchFetcherNum : fetcherNum;
            ExecutorService executor = ThreadPoolUtils.getFixedSizeThreadPool("dynamo-fetcher", num);

            for (int i = 0; i < num; i++) {
                executor.submit(new Fetcher());
            }

            fetchersInitiated.set(true);
        }
    }


    @Override
    protected String lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        String result = null;
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
            result = accountLookupService.batchLookupIds(accountLookupRequest).get(0);
            log.info(String.format(
                    "Fetched result from Dynamo for 1 sync requests (DataCloudVersion=%s) Duration=%d Result is %sempty",
                    request.getMatchTravelerContext().getDataCloudVersion(), System.currentTimeMillis() - startTime,
                    result == null ? "" : "not "));
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

        // Put duns for matched ldc account into traveler for entity match
        // TODO: If it's domain-only match, not able to get duns
        if (result != null) {
            request.getMatchTravelerContext().setDunsOriginMapIfAbsent(new HashMap<>());
            request.getMatchTravelerContext().getDunsOriginMap().put(DataCloudConstants.ACCOUNT_MASTER,
                    matchKeyTuple.getDuns());
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
            while(true) {
                // DataCloudVersion -> ReqIds
                Map<String, List<String>> reqIdsWithVersion = new HashMap<>();
                Map<String, AccountLookupRequest> lookupReqWithVersion = new HashMap<String, AccountLookupRequest>();
                synchronized (pendingReqIds) {
                    while (pendingReqIds.isEmpty()) {
                        try {
                            pendingReqIds.wait();
                        } catch (InterruptedException e) {
                            log.error(String.format("Encounter InterruptedException in Dynamo fetcher: %s",
                                    e.getMessage()));
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
                        List<AccountLookupEntry> results = accountLookupService
                                .batchLookup(lookupReqWithVersion.get(dataCloudVersion));
                        List<String> reqIds = reqIdsWithVersion.get(dataCloudVersion);
                        if (results == null) {
                            throw new RuntimeException(String.format(
                                    "Dynamo lookup got null matching results: submitted %d requests", reqIds.size()));
                        }
                        if (results.size() != reqIds.size()) {
                            throw new RuntimeException(String.format(
                                    "Dynamo lookup failed to return complete matching results: submitted %d requests but only got %d results back",
                                    reqIds.size(), results.size()));
                        }
                        for (int i = 0; i < results.size(); i++) {
                            AccountLookupEntry result = results.get(i);
                            if (result != null && StringUtils.isNotEmpty(result.getLatticeAccountId())) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Got result from lookup for Lookup key=" + reqIds.get(i)
                                            + " Lattice Account Id=" + result);
                                }
                                // Put duns for matched ldc account into
                                // traveler for entity match
                                // TODO: If it's domain-only match, not able to
                                // get duns
                                DataSourceLookupRequest req = getReq(reqIds.get(i));
                                req.getMatchTravelerContext().setDunsOriginMapIfAbsent(new HashMap<>());
                                req.getMatchTravelerContext().getDunsOriginMap().put(DataCloudConstants.ACCOUNT_MASTER,
                                        ((MatchKeyTuple) req.getInputData()).getDuns());
                            } else {
                                // may not be able to handle empty string
                                result = null;
                                if (log.isDebugEnabled()) {
                                    log.debug("Didn't get anything from dynamodb for " + reqIds.get(i));
                                }
                            }
                            String returnAddr = getReqReturnAddr(reqIds.get(i));
                            removeReq(reqIds.get(i));
                            sendResponse(reqIds.get(i), result, returnAddr);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
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
