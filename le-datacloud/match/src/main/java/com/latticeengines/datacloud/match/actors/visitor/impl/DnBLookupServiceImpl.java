package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TERMINATE_EXECUTOR_TIMEOUT_MS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DnBLookupService;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
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
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

@Component("dnbLookupService")
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

    @Value("${datacloud.dnb.status.frequency:120}")
    private int statusFrequency;

    @Value("${datacloud.dnb.batch.fetcher.num:4}")
    private int batchFetchers;

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
    private DnBMatchResultValidator dnbMatchResultValidator;

    @Autowired
    private NameLocationService nameLocationService;

    @Autowired
    private DnbMatchCommandService dnbMatchCommandService;

    private ExecutorService dnbDataSourceServiceExecutor;
    private ExecutorService dnbFetchExecutor;

    private final List<DnBBatchMatchContext> unsubmittedBatches = Collections.synchronizedList(new ArrayList<>());
    private final List<DnBBatchMatchContext> submittedBatches = Collections.synchronizedList(new ArrayList<>());
    private final Queue<DnBBatchMatchContext> finishedBatches = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean batchFetchersInitiated = new AtomicBoolean(false);

    @Autowired
    @Qualifier("dnbBatchScheduler")
    private ThreadPoolTaskScheduler dnbTimerDispatcher;

    @Autowired
    @Qualifier("dnbBatchScheduler")
    private ThreadPoolTaskScheduler dnbTimerStatus;

    // flag to indicate whether background executors should keep running
    private volatile boolean shouldTerminate = false;

    @PostConstruct
    private void postConstruct() {
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
    }

    private void initExecutors() {
        synchronized (batchFetchersInitiated) {
            if (batchFetchersInitiated.get()) {
                // do nothing if fetcher executors are already started
                return;
            }

            log.info("Initialize DnB batch fetcher executors.");
            dnbFetchExecutor = ThreadPoolUtils.getFixedSizeThreadPool("dnb-batch-fetcher", batchFetchers);

            for (int i = 0; i < batchFetchers; i++) {
                dnbFetchExecutor.submit(new BatchFetcher());
            }

            batchFetchersInitiated.set(true);
        }
    }

    @PreDestroy
    private void predestroy() {
        try {
            if (shouldTerminate) {
                return;
            }
            log.info("Shutting down DnB lookup executors");
            shouldTerminate = true;
            if (dnbTimerDispatcher != null) {
                dnbTimerDispatcher.shutdown();
            }
            if (dnbTimerStatus != null) {
                dnbTimerStatus.shutdown();
            }
            if (dnbDataSourceServiceExecutor != null) {
                dnbDataSourceServiceExecutor.shutdownNow();
                dnbDataSourceServiceExecutor.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            if (dnbFetchExecutor != null) {
                dnbFetchExecutor.shutdownNow();
                dnbFetchExecutor.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            log.info("Completed shutting down of DnB lookup executors");
        } catch (Exception e) {
            log.error("Fail to finish all pre-destroy actions", e);
        }
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
        // Inject failure only for testing purpose
        injectFailure(request);
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
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            log.error("Calling/Waiting to call DnB realtime API got timeout after 90s");
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
            // Lazily initialize DnB fetch executor. If no requests to submit,
            // no need to start fetch executor
            if (!batchFetchersInitiated.get()) {
                initExecutors();
            }
            // Failed batch requests to process
            List<DnBBatchMatchContext> failedBatches = new ArrayList<>();
            List<DnBBatchMatchContext> batchesToSubmit = new ArrayList<>();
            // Get batches to submit if meeting one of the requirements:
            // 1. batch size = 10K,
            // 2. batch is sealed,
            // 3. timestamp of last inserted record is 2 mins ago
            // 4. batch request has been created for 30 mins
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
                                || (System.currentTimeMillis() - batchContext.getCreateTime().getTime()) >= 1800000) {
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
                if (batchContext.getDnbCode() == DnBReturnCode.SUBMITTED) {
                    // Successfully submitted batch requests
                    dnbMatchCommandService.dnbMatchCommandCreate(batchContext);
                    submittedBatches.add(batchContext);
                } else if (batchContext.getDnbCode().isScheduledRetryStatus()) {
                    // For these status, wait for next scheduled submission to
                    // retry
                    unsubmittedBatches.add(0, batchContext);
                } else { // Failed batch requests
                    failedBatches.add(batchContext);
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
        if (!batchFetchersInitiated.get()) {
            return;
        }
        try {
            // Batch requests to retry
            List<DnBBatchMatchContext> retryBatches = new ArrayList<>();
            // BatchID -> DnBCode, to find out requests which change status so that DnBMatchCommand table could be updated
            Map<String, DnBReturnCode> changedBatchStatus = new HashMap<>();
            // Batch requests which changed status
            List<DnBBatchMatchContext> changedBatches = new ArrayList<>();
            // Finished requests to fetch result
            List<DnBBatchMatchContext> finishedBatches = new ArrayList<>();
            if (submittedBatches.isEmpty()) {
                return;
            }
            synchronized (submittedBatches) {
                dnbBulkLookupStatusChecker.checkStatus(submittedBatches);
                // If already have retried requests which haven't got response
                // back, don't retry more
                boolean hasRetried = false;
                for (DnBBatchMatchContext batch : submittedBatches) {
                    if (batch.getRetryForServiceBatchId() != null) {
                        hasRetried = true;
                    }
                    changedBatchStatus.put(batch.getServiceBatchId(), batch.getDnbCode());
                }
                Iterator<DnBBatchMatchContext> iter = submittedBatches.iterator();
                while (iter.hasNext()) {
                    DnBBatchMatchContext submittedBatch = iter.next();
                    if (submittedBatch.getDnbCode() != changedBatchStatus.get(submittedBatch.getServiceBatchId())) {
                        changedBatches.add(submittedBatch);
                    }
                    switch (submittedBatch.getDnbCode()) {
                    case OK:
                        finishedBatches.add(submittedBatch);
                        iter.remove();
                        break;
                    case SUBMITTED:
                    case IN_PROGRESS:
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
                    // For any requests failed to check status, don't do
                    // anything and wait for next scheduled check until timeout
                    default:
                        break;
                    }
                }
            }
            // Update DnB command status
            if (CollectionUtils.isNotEmpty(changedBatches)) {
                dnbMatchCommandService.dnbMatchCommandUpdateStatus(changedBatches);
            }
            // Fetch result for finished batch requests
            finishedBatches.forEach(finishedBatch -> this.finishedBatches.offer(finishedBatch));
            synchronized (this.finishedBatches) {
                this.finishedBatches.notify();
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

    private class BatchFetcher implements Runnable {
        @Override
        public void run() {
            while (!shouldTerminate) {
                try {
                    DnBBatchMatchContext finishedBatch = null;
                    synchronized (finishedBatches) {
                        while (!shouldTerminate && finishedBatches.isEmpty()) {
                            try {
                                finishedBatches.wait();
                            } catch (InterruptedException e) {
                                if (!shouldTerminate) {
                                    log.warn("DnB lookup executor (in background) is interrupted");
                                }
                            }
                        }
                        finishedBatch = finishedBatches.poll();
                    }
                    if (finishedBatch != null) {
                        dnbBatchFetchResult(finishedBatch);
                    }
                } catch (Exception ex) {
                    log.error("Exception in fetching dnb batch request result", ex);
                }

            }
        }
    }

    private void dnbBatchFetchResult(DnBBatchMatchContext batch) {
        try {
            try {
                batch = dnbBulkLookupFetcher.getResult(batch);
            } catch (Exception ex) {
                log.error(String.format("Fail to poll match result for request %s from DnB bulk matchc service: %s",
                        batch.getServiceBatchId(), ex.getMessage()));
                batch.setDnbCode(DnBReturnCode.UNKNOWN);
            }
            if (batch.getDnbCode() == DnBReturnCode.OK) {
                // Successful batch request
                processBulkMatchResult(batch, true, true);
            } else if (batch.getDnbCode().isScheduledRetryStatus()) {
                // For these status, wait for next scheduled fetch to
                // retry
                finishedBatches.offer(batch);
            } else {
                // Failed batch request
                processBulkMatchResult(batch, false, true);
            }
        } catch (Exception ex) {
            log.error("Exception in fetching dnb batch request result", ex);
            processBulkMatchResult(batch, false, true);
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
        log.info(String.format("Start processing DnB batch result: ServiceBatchId=%s RootOperationUID=%s",
                batchContext.getServiceBatchId(), batchContext.getRootOperationUid()));
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
        log.info(String.format("Start writing DnB batch result to match history: ServiceBatchId=%s RootOperationUID=%s",
                batchContext.getServiceBatchId(), batchContext.getRootOperationUid()));
        writeDnBMatchHistory(dnBMatchHistories);
        log.info(String.format(
                "Finished processing DnB batch: ServiceBatchId=%s RootOperationUID=%s, StartTime=%s, FinishTime=%s, Size=%d, Duration=%d mins",
                batchContext.getServiceBatchId(), batchContext.getRootOperationUid(), batchContext.getTimestamp(),
                finishTime, batchContext.getContexts().size(),
                (finishTime.getTime() - batchContext.getTimestamp().getTime()) / 60000));
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

        context.setDunsInAM(true);
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

    @Override
    public Map<String, Integer> getRealtimeReqStats() {
        Map<String, Integer> res = new HashMap<>();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) dnbDataSourceServiceExecutor;
        res.put(MatchConstants.ACTIVE_REQ_NUM, executor == null ? 0 : executor.getActiveCount());
        res.put(MatchConstants.QUEUED_REQ_NUM,
                executor == null || executor.getQueue() == null ? 0 : executor.getQueue().size());
        return res;
    }

}
