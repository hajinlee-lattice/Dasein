package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DnBLookupService;
import com.latticeengines.datacloud.match.domain.TpsLookupResult;
import com.latticeengines.datacloud.match.entitymgr.ContactTpsLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.ContactTpsLookupEntryMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.match.ContactTpsLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("tpsLookupService")
public class TpsLookupService extends DataSourceLookupServiceBase implements DnBLookupService {
    private static final Logger log = LoggerFactory.getLogger(TpsLookupService.class);

    private volatile ExecutorService executor;

    private Map<String, TpsLookupResult.ReturnCode> mockErrorDuns = new HashMap<>();
    private Random random = new Random(4);

    private Map<String, ContactTpsLookupEntryMgr> lookupMgrs = new HashMap<>();

    @Inject
    private FabricDataService dataService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private MatchActorSystem actorSystem;

    @Override
    public Map<String, Integer> getUnsubmittedStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Integer> getSubmittedStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Integer> getFinishedStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Integer> getRealtimeReqStats() {
        throw new UnsupportedOperationException();
    }

    // return TpsLookupResult
    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        String duns = matchKeyTuple.getDuns();
        if (StringUtils.isBlank(duns)) {
            return getEmptyResult();
        } else {
            TpsLookupResult result = new TpsLookupResult();
            if (mockErrorDuns.containsKey(duns)) {
                // mock error for testing
                TpsLookupResult.ReturnCode error = mockErrorDuns.get(duns);
                result.setReturnCode(error);
                log.info("Inject {} error for duns {}", error, duns);
                return result;
            } else {
                Collection<String> recordIds;
                try {
                    recordIds = lookupResults(duns);
                } catch (Exception e) {
                    log.error("Failed to lookup tps record ids using duns {}", duns, e);
                    result.setReturnCode(TpsLookupResult.ReturnCode.UnknownRemoteError);
                    return result;
                }

                if (CollectionUtils.isEmpty(recordIds)) {
                    return getEmptyResult();
                } else {
                    result.setRecordIds(new ArrayList<>(recordIds));
                    result.setReturnCode(TpsLookupResult.ReturnCode.Ok);
                    return result;
                }
            }
        }
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        ExecutorService executor = getExecutor();
        executor.submit(() -> {
            Object result = lookupFromService(lookupRequestId, request);
            sendResponse(lookupRequestId, result, returnAddress);
        });
    }

    protected void sendResponse(String lookupRequestId, Object result, String returnAddress) {
        Response response = new Response();
        response.setRequestId(lookupRequestId);
        response.setResult(result);
        actorSystem.sendResponse(response, returnAddress);
        if (log.isDebugEnabled()) {
            log.info(String.format("Returned response for %s to %s", lookupRequestId, returnAddress));
        }
    }

    private ExecutorService getExecutor() {
        if (executor == null) {
            initExecutor();
        }
        return executor;
    }

    private synchronized void initExecutor() {
        if (executor == null) {
            log.info("Initialize tps lookup thread pool.");
            BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<Runnable>();
            executor = new ThreadPoolExecutor(1, 16, 1, TimeUnit.MINUTES, runnableQueue);
        }
    }

    private TpsLookupResult getEmptyResult() {
        TpsLookupResult result = new TpsLookupResult();
        result.setRecordIds(Collections.emptyList());
        result.setReturnCode(TpsLookupResult.ReturnCode.EmptyResult);
        return result;
    }

    private Collection<String> lookupResults(String duns) {
        Set<String> recordIds = new HashSet<>();
        ContactTpsLookupEntryMgr lookupMgr = getLookupMgr();
        ContactTpsLookupEntry entry = lookupMgr.findByKey(duns);
        if (entry != null) {
            String[] uuids = entry.getRecordUuids().split(",");
            for (String id : uuids) {
                recordIds.add(id);
            }
        } else {
            log.warn("Can't find a match in datacloud for duns {}", duns);
        }

        return recordIds;
    }

    public ContactTpsLookupEntryMgr getLookupMgr() {
        String version = versionEntityMgr.currentApprovedVersion().getVersion();
        log.info("TpsLookupService, datacloud version " + version);
        ContactTpsLookupEntryMgr lookupMgr = lookupMgrs.get(version);
        if (lookupMgr == null) {
            lookupMgr = new ContactTpsLookupEntryMgrImpl(dataService, version);
            lookupMgr.init();
            lookupMgrs.put(version, lookupMgr);
        }
        return lookupMgr;
    }

    private Collection<String> lookupFakeResults(String duns) {
        Set<String> recordIds = new HashSet<>();
        int numRecordIds = 1 + random.nextInt(999); // 1 to 999 record ids for this duns
        for (int i = 0; i < numRecordIds; i++) {
            recordIds.add(String.format("%04d", random.nextInt(10000)));
        }
        return recordIds;
    }

    @VisibleForTesting
    void addMockError(String duns, TpsLookupResult.ReturnCode errorCode) {
        mockErrorDuns.put(duns, errorCode);
    }

}
