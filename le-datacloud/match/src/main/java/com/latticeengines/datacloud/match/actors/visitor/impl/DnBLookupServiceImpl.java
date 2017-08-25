package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DnBLookupService;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.service.DnbMatchCommandService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase implements DnBLookupService {
    private static final Logger log = LoggerFactory.getLogger(DnBLookupServiceImpl.class);

    @Value("${datacould.dnb.realtime.timeout.minute}")
    private long realtimeTimeoutMinute;

    @Value("${datacloud.dnb.bulk.timeout.minute}")
    private long bulkTimeoutMinute;

    private long realtimeTimeout;

    private long bulkTimeout;

    @Value("${datacloud.dnb.bulk.request.maximum}")
    private int maximumBatchSize;

    @Value("${datacloud.match.actor.datasource.dnb.threadpool.count.min}")
    private Integer dnbThreadpoolCountMin;

    @Value("${datacloud.match.actor.datasource.dnb.threadpool.count.max}")
    private Integer dnbThreadpoolCountMax;

    @Value("${datacloud.match.actor.datasource.dnb.api.call.maxwait}")
    private Integer dnbApiCallMaxWait;

    @Value("${datacloud.dnb.bulk.retry.times}")
    private int bulkRetryTimes;

    @Value("${datacloud.dnb.bulk.retry.wait.minute}")
    private long bulkRetryWaitMinute;

    private long bulkRetryWait;

    @Value("${datacloud.dnb.bulk.retry.pendingrecord.threshold}")
    private int bulkRetryPendingRecordThreshold;

    @Value("${datacloud.dnb.dispatcher.frequency.sec:30}")
    private int dispatcherFrequency;

    @Value("${datacloud.dnb.fetcher.frequency.sec:20}")
    private int fetcherFrequency;

    @Value("${datacloud.dnb.status.frequency:120}")
    private int statusFrequency;

    @Value("${datacloud.dnb.bulk.redirect.realtime.threshold:100}")
    private int bulkToRealtimeThreshold;

    @Autowired
    private DnBRealTimeLookupService dnbRealTimeLookupService;

    @Autowired
    private DnBBulkLookupDispatcher dnbBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnbBulkLookupFetcher;

    @Autowired
    private DnBBulkLookupStatusChecker dnbBulkLookupStatusChecker;

    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

    @Autowired
    private NameLocationService nameLocationService;

    @Autowired
    private DnbMatchCommandService dnbMatchCommandService;

    private ExecutorService dnbDataSourceServiceExecutor;

    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> unsubmittedBatches = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> submittedBatches = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> finishedBatches = Collections.synchronizedList(new ArrayList<>());

    @Autowired
    @Qualifier("dnbBatchScheduler")
    private ThreadPoolTaskScheduler dnbTimerDispatcher;

    @Autowired
    @Qualifier("dnbBatchScheduler")
    private ThreadPoolTaskScheduler dnbTimerStatus;

    @Autowired
    @Qualifier("dnbBatchScheduler")
    private ThreadPoolTaskScheduler dnbTimerFetcher;

    @PostConstruct
    public void postConstruct() {
        initDnBDataSourceThreadPool();
        realtimeTimeout = realtimeTimeoutMinute * 60 * 1000;
        bulkTimeout = bulkTimeoutMinute * 60 * 1000;
        bulkRetryWait = bulkRetryWaitMinute * 60 * 1000;

        dnbTimerDispatcher.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                dnbBatchDispatchRequest();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(dispatcherFrequency)),
                TimeUnit.SECONDS.toMillis(dispatcherFrequency));

        dnbTimerStatus.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                dnbBatchCheckStatus();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(statusFrequency)),
                TimeUnit.SECONDS.toMillis(statusFrequency));

        dnbTimerFetcher.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                dnbBatchFetchResult();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(fetcherFrequency)),
                TimeUnit.SECONDS.toMillis(fetcherFrequency));
    }

    @PreDestroy
    public void preDestroy() {
        dnbDataSourceServiceExecutor.shutdown();
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        if (!isBatchMode()) {
            DnBMatchContext result = (DnBMatchContext) lookupFromService(lookupRequestId, request);
            sendResponse(lookupRequestId, result, returnAddress);
        } else {
            acceptBulkLookup(lookupRequestId, request, returnAddress);
        }
    }

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext context = new DnBMatchContext();
        context.setLookupRequestId(lookupRequestId);
        context.setInputNameLocation(matchKeyTuple);
        nameLocationService.setDefaultCountry(context.getInputNameLocation());
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        boolean readyToReturn = false;
        // Check timeout
        if ((System.currentTimeMillis() - request.getTimestamp()) >= realtimeTimeout) {
            context.setDnbCode(DnBReturnCode.UNMATCH_TIMEOUT);
            readyToReturn = true;
        }
        // Check country code
        if (!readyToReturn && StringUtils.isEmpty(context.getInputNameLocation().getCountryCode())) {
            context.setDnbCode(DnBReturnCode.UNMATCH);
            readyToReturn = true;
        }

        MatchTraveler traveler = request.getMatchTravelerContext();
        context.setDataCloudVersion(traveler.getDataCloudVersion());
        context.setRootOperationUid(traveler.getMatchInput().getRootOperationUid());
        if (!readyToReturn && traveler.getMatchInput().isUseDnBCache()) {
            Long startTime = System.currentTimeMillis();
            DnBCache cache = dnbCacheService.lookupCache(context);
            if (cache != null) {
                if (cache.isWhiteCache()) {
                    if ((request.getMatchTravelerContext().getMatchInput() != null
                            && request.getMatchTravelerContext().getMatchInput().isDisableDunsValidation())
                            || adoptWhiteCache(cache, context.getDataCloudVersion())) {
                        context.copyResultFromCache(cache);
                        dnbMatchResultValidator.validate(context);
                        log.info(String.format(
                                "Found DnB match context in white cache%s: Name=%s, Country=%s, State=%s, City=%s, "
                                        + "ZipCode=%s, PhoneNumber=%s, DUNS=%s, ConfidenceCode=%d, MatchGrade=%s, "
                                        + "OutOfBusiness=%s, IsDunsInAM=%s, Duration=%d",
                                context.getRootOperationUid() == null ? ""
                                        : " (RootOperationID=" + context.getRootOperationUid() + ")",
                                context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                                context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                                context.getInputNameLocation().getZipcode(),
                                context.getInputNameLocation().getPhoneNumber(), context.getDuns(),
                                context.getConfidenceCode(), context.getMatchGrade().getRawCode(),
                                context.isOutOfBusinessString(), context.isDunsInAMString(),
                                System.currentTimeMillis() - startTime));
                        readyToReturn = true;
                    } else {
                        log.info(String.format(
                                "Reject invalid white cache: Id=%s DUNS=%s OutOfBusiness=%s IsDunsInAM=%s",
                                cache.getId(), cache.getDuns(), cache.isOutOfBusinessString(),
                                cache.isDunsInAMString()));
                    }
                } else {
                    context.copyResultFromCache(cache);
                    log.info(String.format(
                            "Found DnB match context in black cache%s: Name=%s, Country=%s, State=%s, City=%s, "
                                    + "ZipCode=%s, PhoneNumber=%s, Duration=%d",
                            context.getRootOperationUid() == null ? ""
                                    : " (RootOperationID=" + context.getRootOperationUid() + ")",
                            context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                            context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                            context.getInputNameLocation().getZipcode(),
                            context.getInputNameLocation().getPhoneNumber(), System.currentTimeMillis() - startTime));
                    readyToReturn = true;
                }

            }
        }
        if (!readyToReturn && Boolean.TRUE.equals(traveler.getMatchInput().getUseRemoteDnB())) {
            context = dnbRealtimeLookup(context);
            readyToReturn = true;
        }

        if (readyToReturn) {
            List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
            dnBMatchHistories.add(new DnBMatchHistory(context));
            writeDnBMatchHistory(dnBMatchHistories);
        }
        context.setDuration(System.currentTimeMillis() - request.getTimestamp());
        return context;
    }

    protected void acceptBulkLookup(String lookupRequestId, DataSourceLookupRequest request, String returnAddress) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext context = new DnBMatchContext();
        context.setLookupRequestId(lookupRequestId);
        context.setInputNameLocation(matchKeyTuple);
        nameLocationService.setDefaultCountry(context.getInputNameLocation());
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
        boolean readyToReturn = false;
        // Check timeout
        if ((System.currentTimeMillis() - request.getTimestamp()) >= bulkTimeout) {
            context.setDnbCode(DnBReturnCode.UNMATCH_TIMEOUT);
            readyToReturn = true;
        }
        // Check country code
        if (!readyToReturn && StringUtils.isEmpty(context.getInputNameLocation().getCountryCode())) {
            context.setDnbCode(DnBReturnCode.UNMATCH);
            readyToReturn = true;
        }
        context.setTimestamp(request.getTimestamp());

        MatchTraveler traveler = request.getMatchTravelerContext();
        context.setDataCloudVersion(traveler.getDataCloudVersion());
        context.setLogDnBBulkResult(traveler.getMatchInput().isLogDnBBulkResult());
        context.setRootOperationUid(traveler.getMatchInput().getRootOperationUid());
        if (!readyToReturn && traveler.getMatchInput().isUseDnBCache()) {
            Long startTime = System.currentTimeMillis();
            DnBCache cache = dnbCacheService.lookupCache(context);
            if (cache != null) {
                if (cache.isWhiteCache()) {
                    if ((request.getMatchTravelerContext().getMatchInput() != null
                            && request.getMatchTravelerContext().getMatchInput().isDisableDunsValidation())
                            || adoptWhiteCache(cache, context.getDataCloudVersion())) {
                        context.copyResultFromCache(cache);
                        dnbMatchResultValidator.validate(context);
                        if (context.getLogDnBBulkResult()) {
                            log.info(String.format(
                                    "Found DnB match context in white cache: Name=%s, Country=%s, State=%s, City=%s, "
                                            + "ZipCode=%s, PhoneNumber=%s, DUNS=%s, ConfidenceCode=%d, MatchGrade=%s, "
                                            + "OutOfBusiness=%s, IsDunsInAM=%s, Duration=%d",
                                    context.getInputNameLocation().getName(),
                                    context.getInputNameLocation().getCountry(),
                                    context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                                    context.getInputNameLocation().getZipcode(),
                                    context.getInputNameLocation().getPhoneNumber(), context.getDuns(),
                                    context.getConfidenceCode(), context.getMatchGrade().getRawCode(),
                                    context.isOutOfBusinessString(), context.isDunsInAMString(),
                                    System.currentTimeMillis() - startTime));
                        }
                        readyToReturn = true;
                    } else {
                        log.info(String.format(
                                "Reject invalid white cache: Id=%s DUNS=%s OutOfBusiness=%s IsDunsInAM=%s",
                                cache.getId(), cache.getDuns(), cache.isOutOfBusinessString(),
                                cache.isDunsInAMString()));
                    }
                } else {
                    context.copyResultFromCache(cache);
                    if (context.getLogDnBBulkResult()) {
                        log.info(String.format(
                                "Found DnB match context in black cache: Name=%s, Country=%s, State=%s, City=%s, "
                                        + "ZipCode=%s, PhoneNumber=%s, Duration=%d",
                                context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                                context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                                context.getInputNameLocation().getZipcode(),
                                context.getInputNameLocation().getPhoneNumber(),
                                System.currentTimeMillis() - startTime));
                    }
                    readyToReturn = true;
                }

            }
        }

        if (!readyToReturn && Boolean.TRUE.equals(traveler.getMatchInput().getUseRemoteDnB())) {
            saveReq(lookupRequestId, returnAddress, request);
            // Bucket single contexts to batched contexts in unsubmittedReqs
            synchronized (unsubmittedBatches) {
                // If unsubmittedBatches is empty, or last unsubmitted batch is sealed, 
                // or last unsubmitted batch size is 10K, create a new batch
                if (unsubmittedBatches.isEmpty() || unsubmittedBatches.get(unsubmittedBatches.size() - 1).isSealed()
                        || unsubmittedBatches.get(unsubmittedBatches.size() - 1).getContexts()
                                .size() == maximumBatchSize) {
                    DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
                    batchContext.setLogDnBBulkResult(context.getLogDnBBulkResult());
                    batchContext.setRootOperationUid(context.getRootOperationUid());
                    unsubmittedBatches.add(batchContext);
                }
                unsubmittedBatches.get(unsubmittedBatches.size() - 1).getContexts().put(context.getLookupRequestId(),
                        context);
                unsubmittedBatches.get(unsubmittedBatches.size() - 1).setTimestamp(new Date());
            }
        } else {
            context.setDuration(System.currentTimeMillis() - request.getTimestamp());
            sendResponse(lookupRequestId, context, returnAddress);
        }
    }

    private DnBMatchContext dnbRealtimeLookup(DnBMatchContext context) {
        context.setCalledRemoteDnB(true);
        context.setRequestTime(new Date());
        Callable<DnBMatchContext> task = createCallableForRemoteDnBApiCall(context);
        Future<DnBMatchContext> dnbFuture = dnbDataSourceServiceExecutor.submit(task);

        try {
            context = dnbFuture.get(dnbApiCallMaxWait, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        context.setResponseTime(new Date());
        validateDuns(context);
        dnbMatchResultValidator.validate(context);
        // Sync write to DnB cache for realtime service for 2 reasons:
        // 1. Performance will not degrade much
        // 2. Patch service needs it
        DnBCache dnBCache = dnbCacheService.addCache(context, true);
        if (dnBCache != null) {
            context.setCacheId(dnBCache.getId());
        }
        return context;
    }

    private void dnbBatchRedirectToRealtime(DnBBatchMatchContext batchContext) {
        log.info(String.format("Batch request %s, size=%d, smaller than threshold %d, redirecting to realtime lookup..",
                batchContext.getRootOperationUid(), batchContext.getContexts().size(), bulkToRealtimeThreshold));
        long duration = 0;
        for (DnBMatchContext context : batchContext.getContexts().values()) {
            dnbRealtimeLookup(context);
            duration += context.getDuration() != null ? context.getDuration() : 0;
        }
        batchContext.setDuration(duration == 0 ? null : duration);
        batchContext.setDnbCode(DnBReturnCode.OK);
        log.info(String.format("Batch request %s finished using realtime lookup", batchContext.getRootOperationUid()));
        processBulkMatchResult(batchContext, true, false);
    }

    private void dnbBatchDispatchRequest() {
        try {
            if (unsubmittedBatches.isEmpty()) {
                return;
            }
            // Failed batch requests to process
            List<DnBBatchMatchContext> failedBatches = new ArrayList<>();
            List<DnBBatchMatchContext> batchesToSubmit = new ArrayList<>();
            // Get batches to submit:
            // batch size = 10K, or batch is sealed, or timestamp of last inserted record is 2 mins ago
            synchronized (unsubmittedBatches) {
                int unsubmittedNum = getUnsubmittedStats().get(MatchConstants.REQUEST_NUM);
                if (unsubmittedNum > 0) {
                    log.info(String.format("There are %d records unsubmitted before request dispatching",
                            unsubmittedNum));
                    Iterator<DnBBatchMatchContext> iter = unsubmittedBatches.iterator();
                    while (iter.hasNext()) {
                        DnBBatchMatchContext batchContext = iter.next();
                        if (batchContext.isSealed() || batchContext.getContexts().size() == maximumBatchSize
                                || (System.currentTimeMillis() - batchContext.getTimestamp().getTime()) >= 120000
                                || (System.currentTimeMillis() - batchContext.getCreateTime().getTime()) >= 300000) {
                            batchContext.setSealed(true);
                            batchesToSubmit.add(batchContext);
                            iter.remove();
                        }
                    }
                }
            }
            // Dispatch batch requests
            for (DnBBatchMatchContext batchContext : batchesToSubmit) {
                if (batchContext.getContexts().size() <= bulkToRealtimeThreshold) {
                    Runnable task = createBatchToRealtimeRunnable(batchContext);
                    dnbDataSourceServiceExecutor.execute(task);
                    continue;
                }
                try {
                    batchContext = dnbBulkLookupDispatcher.sendRequest(batchContext);
                } catch (Exception ex) {
                    log.error(String.format("Exception in dispatching match requests to DnB bulk match service: %s",
                            ex.getMessage()));
                    batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
                }
                switch (batchContext.getDnbCode()) {
                case SUBMITTED:
                    dnbMatchCommandService.dnbMatchCommandCreate(batchContext);
                    submittedBatches.add(batchContext);
                    break;
                case RATE_LIMITING:
                case EXCEED_LIMIT_OR_UNAUTHORIZED:
                    // Not allow to submit this request due to rate limiting
                    // Put it back to unsubmittedReqs list.
                    unsubmittedBatches.add(0, batchContext);
                    break;
                default:
                    failedBatches.add(batchContext);
                    break;
                }
            }
            int unsubmittedNum = getUnsubmittedStats().get(MatchConstants.REQUEST_NUM);
            if (unsubmittedNum > 0) {
                log.info(String.format("There are %d records unsubmitted after request dispatching", unsubmittedNum));
            }
            // Process failed batch requests
            for (DnBBatchMatchContext batchContext : failedBatches) {
                processBulkMatchResult(batchContext, false, true);
            }
        } catch (Exception ex) {
            log.error("Exception in dispatching dnb batch requests", ex);
        }
    }

    private void dnbBatchCheckStatus() {
        try {
            // Failed batch requests to process
            List<DnBBatchMatchContext> failedBatches = new ArrayList<>();
            // Batch requests to retry
            List<DnBBatchMatchContext> retryBatches = new ArrayList<>();
            if (submittedBatches.isEmpty()) {
                return;
            }
            synchronized (submittedBatches) {
                dnbBulkLookupStatusChecker.checkStatus(submittedBatches);
                boolean hasRetried = false;
                for (DnBBatchMatchContext batch : submittedBatches) {
                    if (batch.getRetryForServiceBatchId() != null) {
                        hasRetried = true;
                        break;
                    }
                }
                Iterator<DnBBatchMatchContext> iter = submittedBatches.iterator();
                while (iter.hasNext()) {
                    DnBBatchMatchContext submittedBatch = iter.next();
                    switch (submittedBatch.getDnbCode()) {
                    case OK:
                        finishedBatches.add(submittedBatch);
                        iter.remove();
                        break;
                    case SUBMITTED:
                    case IN_PROGRESS:
                    case RATE_LIMITING:
                    case EXCEED_LIMIT_OR_UNAUTHORIZED:
                        if (submittedBatch.getRetryTimes() < bulkRetryTimes && submittedBatch.getTimestamp() != null
                                && (System.currentTimeMillis()
                                        - submittedBatch.getTimestamp().getTime() >= bulkRetryWait)) {
                            if (!hasRetried) {
                                retryBatches.add(submittedBatch);
                            } else {
                                long mins = (System.currentTimeMillis() - submittedBatch.getTimestamp().getTime()) / 60
                                        / 1000;
                                log.info(String.format(
                                        "Batch request %s was submitted %d minutes ago, but currently there has been one retried batch submitted, skip retry this time.",
                                        submittedBatch.getServiceBatchId(), mins));
                            }
                        }
                        break;
                    default:
                        failedBatches.add(submittedBatch);
                        iter.remove();
                        break;
                    }
                }
            }
            // Process failed batch requests
            for (DnBBatchMatchContext batchContext : failedBatches) {
                processBulkMatchResult(batchContext, false, true);
            }
            // Retry batch requests
            for (DnBBatchMatchContext retryBatch : retryBatches) {
                long mins = (System.currentTimeMillis() - retryBatch.getTimestamp().getTime()) / 60 / 1000;
                log.info(String.format("Batch request %s was submitted %d mins ago. Retry it!",
                        retryBatch.getServiceBatchId(), mins));
                retryBulkRequest(retryBatch);
            }
            if (submittedBatches.size() > 0) {
                log.info(String.format("There are %d batch requests waiting for DnB batch api to return results",
                        submittedBatches.size()));
            }
        } catch (Exception ex) {
            log.error("Exception in checking dnb batch request status", ex);
        }
    }

    private void dnbBatchFetchResult() {
        try {
            // Failed batch requests to process
            List<DnBBatchMatchContext> failedBatches = new ArrayList<>();
            // Success batch requests to process
            List<DnBBatchMatchContext> successBatches = new ArrayList<>();
            if (finishedBatches.isEmpty()) {
                return;
            }
            synchronized (finishedBatches) {
                Iterator<DnBBatchMatchContext> iter = finishedBatches.iterator();
                while (iter.hasNext()) {
                    DnBBatchMatchContext finishedBatch = iter.next();
                    try {
                        finishedBatch = dnbBulkLookupFetcher.getResult(finishedBatch);
                    } catch (Exception ex) {
                        log.error(String.format(
                                "Fail to poll match result for request %s from DnB bulk matchc service: %s",
                                finishedBatch.getServiceBatchId(), ex.getMessage()));
                        finishedBatch.setDnbCode(DnBReturnCode.UNKNOWN);
                    }
                    switch (finishedBatch.getDnbCode()) {
                    case OK:
                        successBatches.add(finishedBatch);
                        iter.remove();
                        break;
                    default:
                        failedBatches.add(finishedBatch);
                        iter.remove();
                        break;
                    }
                }
            }
            // Process failed batch requests
            for (DnBBatchMatchContext batchContext : failedBatches) {
                processBulkMatchResult(batchContext, false, true);
            }
            // Process success batch requests
            for (DnBBatchMatchContext batchContext : successBatches) {
                processBulkMatchResult(batchContext, true, true);
            }
        } catch (Exception ex) {
            log.error("Exception in fetching dnb batch request result", ex);
        }
    }

    private void retryBulkRequest(DnBBatchMatchContext batchContext) {
        batchContext.setRetryTimes(batchContext.getRetryTimes() + 1);
        DnBBatchMatchContext retryContext = new DnBBatchMatchContext();
        retryContext.copyForRetry(batchContext);
        unsubmittedBatches.add(0, retryContext);
    }

    private void processBulkMatchResult(DnBBatchMatchContext batchContext, boolean success,
            boolean postProcessDnBContexts) {
        List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
        Date finishTime = new Date();
        for (String lookupRequestId : batchContext.getContexts().keySet()) {
            String returnAddr = getReqReturnAddr(lookupRequestId);
            if (returnAddr == null) {
                log.info(String.format(
                        "Result of record (lookupRequestId=%s) has been returned. serviceBatchId=%s%s. Do not return for 2nd time.",
                        lookupRequestId, batchContext.getServiceBatchId(),
                        batchContext.getRetryForServiceBatchId() == null ? ""
                                : " (retry for " + batchContext.getRetryForServiceBatchId() + ")"));
                continue;
            }
            DnBMatchContext context = batchContext.getContexts().get(lookupRequestId);
            if (postProcessDnBContexts) {
                context.setCalledRemoteDnB(true);
                context.setRequestTime(batchContext.getTimestamp());
                context.setResponseTime(finishTime);
                if (!success) {
                    context.setDnbCode(batchContext.getDnbCode());
                } else {
                    validateDuns(context);
                    dnbMatchResultValidator.validate(context);
                    dnbCacheService.addCache(context, false);
                }
            }
            removeReq(lookupRequestId);
            sendResponse(lookupRequestId, context, returnAddr);
            dnBMatchHistories.add(new DnBMatchHistory(context));
        }
        dnbMatchCommandService.dnbMatchCommandUpdate(batchContext);
        writeDnBMatchHistory(dnBMatchHistories);
        log.info(String.format(
                "Finished processing DnB batch: ServiceBatchId=%s RootOperationUID=%s, StartTime=%s, FinishTime=%s, Size=%d, Duration=%d mins",
                batchContext.getServiceBatchId(), batchContext.getRootOperationUid(), batchContext.getTimestamp(),
                finishTime, batchContext.getContexts().size(),
                (finishTime.getTime() - batchContext.getTimestamp().getTime()) / 60000));
    }

    /**
     * Pre-validation before DnB remote lookup
     */
    private boolean adoptWhiteCache(DnBCache cache, String dataCloudVersion) {
        if (cache.isDunsInAM() == null) {
            return false;
        }
        if (Boolean.TRUE.equals(cache.isOutOfBusiness())) {
            return true;
        }
        boolean currentIsDunsInAM = isDunsInAM(cache.getDuns(), dataCloudVersion);
        if (currentIsDunsInAM == cache.isDunsInAM().booleanValue()) {
            return true;    // If adopted cache has isDunsInAM == false, it will be discarded in DnBMatchResultValidator
        } else {
            return false;
        }
    }

    /**
     * Post-validation after DnB remote lookup
     */
    private void validateDuns(DnBMatchContext context) {
        if (StringUtils.isEmpty(context.getDuns())) {
            context.setDunsInAM(Boolean.FALSE);
            return;
        }
        if (Boolean.TRUE.equals(context.isOutOfBusiness())) {
            context.setDunsInAM(Boolean.FALSE);
            return;
        }
        if (isDunsInAM(context.getDuns(), context.getDataCloudVersion())) {
            context.setDunsInAM(Boolean.TRUE);
        } else {
            context.setDunsInAM(Boolean.FALSE);
        }
    }

    private boolean isDunsInAM(String duns, String dataCloudVersion) {
        AccountLookupRequest lookupRequest = new AccountLookupRequest(dataCloudVersion);
        lookupRequest.addLookupPair(null, duns);
        List<String> ids = accountLookupService.batchLookupIds(lookupRequest);
        return (ids != null && ids.size() == 1 && StringUtils.isNotEmpty(ids.get(0)));
    }

    private void writeDnBMatchHistory(List<DnBMatchHistory> metrics) {
        try {
            MeasurementMessage<DnBMatchHistory> message = new MeasurementMessage<>();
            message.setMeasurements(metrics);
            message.setMetricDB(MetricDB.LDC_Match);
            getActorSystem().getMetricActor().tell(message, null);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }

    private void initDnBDataSourceThreadPool() {
        log.info("Initialize dnb data source thread pool.");
        BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<Runnable>();
        dnbDataSourceServiceExecutor = new ThreadPoolExecutor(dnbThreadpoolCountMin, dnbThreadpoolCountMax, 1,
                TimeUnit.MINUTES, runnableQueue);
    }

    private Callable<DnBMatchContext> createCallableForRemoteDnBApiCall(final DnBMatchContext context) {
        Callable<DnBMatchContext> task = new Callable<DnBMatchContext>() {

            @Override
            public DnBMatchContext call() throws Exception {
                DnBMatchContext returnedContext = context;
                try {
                    returnedContext = dnbRealTimeLookupService.realtimeEntityLookup(context);
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                }
                return returnedContext;
            }
        };
        return task;
    }

    private Runnable createBatchToRealtimeRunnable(final DnBBatchMatchContext batchContext) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                dnbBatchRedirectToRealtime(batchContext);
            }
        };
        return task;
    }

    /************************ DnB Lookup Service Status ************************/
    @Override
    public Map<String, Integer> getUnsubmittedStats() {
        Map<String, Integer> res = new HashMap<>();
        synchronized (unsubmittedBatches) {
            res.put(MatchConstants.BATCH_NUM, unsubmittedBatches.size());
            int requestNum = 0;
            for (DnBBatchMatchContext batchContext : unsubmittedBatches) {
                requestNum += batchContext.getContexts().size();
            }
            res.put(MatchConstants.REQUEST_NUM, requestNum);
        }
        return res;
    }

    @Override
    public Map<String, Integer> getSubmittedStats() {
        Map<String, Integer> res = new HashMap<>();
        synchronized (submittedBatches) {
            res.put(MatchConstants.BATCH_NUM, submittedBatches.size());
            int requestNum = 0;
            for (DnBBatchMatchContext batchContext : submittedBatches) {
                requestNum += batchContext.getContexts().size();
            }
            res.put(MatchConstants.REQUEST_NUM, requestNum);
        }
        return res;
    }

    @Override
    public Map<String, Integer> getFinishedStats() {
        Map<String, Integer> res = new HashMap<>();
        synchronized (finishedBatches) {
            res.put(MatchConstants.BATCH_NUM, finishedBatches.size());
            int requestNum = 0;
            for (DnBBatchMatchContext batchContext : finishedBatches) {
                requestNum += batchContext.getContexts().size();
            }
            res.put(MatchConstants.REQUEST_NUM, requestNum);
        }
        return res;
    }

}
