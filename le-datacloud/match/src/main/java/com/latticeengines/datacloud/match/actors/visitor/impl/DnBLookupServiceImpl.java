package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupStatusChecker;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBatchMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

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

    private ExecutorService dnbDataSourceServiceExecutor;

    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> unsubmittedBatches = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> submittedBatches = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> finishedBatches = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings({ "unchecked", "unused" })
    private final Map<String, DnBBatchMatchContext> lookupReqIdToBatchMap = Collections
            .synchronizedMap(new HashMap<>());

    private static AtomicInteger previousUnsubmittedNum = new AtomicInteger(0);

    @PostConstruct
    public void postConstruct() {
        initDnBDataSourceThreadPool();
        realtimeTimeout = realtimeTimeoutMinute * 60 * 1000;
        bulkTimeout = bulkTimeoutMinute * 60 * 1000;
        bulkRetryWait = bulkRetryWaitMinute * 60 * 1000;
    }

    @PreDestroy
    public void preDestroy() {
        dnbDataSourceServiceExecutor.shutdown();
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
                                "Found DnB match context in white cache: Name = %s, Country = %s, State = %s, City = %s, "
                                        + "ZipCode = %s, PhoneNumber = %s, DUNS = %s, ConfidenceCode = %d, MatchGrade = %s, "
                                        + "OutOfBusiness = %s, IsDunsInAM = %s, Duration = %d",
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
                                "Remove invalid white cache: Id=%s DUNS=%s OutOfBusiness=%s IsDunsInAM=%s",
                                cache.getId(), cache.getDuns(), cache.isOutOfBusinessString(),
                                cache.isDunsInAMString()));
                        dnbCacheService.removeCache(cache);
                    }
                } else {
                    context.copyResultFromCache(cache);
                    log.info(String.format(
                            "Found DnB match context in black cache: Name = %s, Country = %s, State = %s, City = %s, "
                                    + "ZipCode = %s, PhoneNumber = %s, Duration = %d",
                            context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                            context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                            context.getInputNameLocation().getZipcode(),
                            context.getInputNameLocation().getPhoneNumber(), System.currentTimeMillis() - startTime));
                    readyToReturn = true;
                }

            }
        }
        if (!readyToReturn && Boolean.TRUE.equals(traveler.getMatchInput().getUseRemoteDnB())) {
            Callable<DnBMatchContext> task = createCallableForRemoteDnBApiCall(context);
            Future<DnBMatchContext> dnbFuture = dnbDataSourceServiceExecutor.submit(task);

            try {
                context = dnbFuture.get(dnbApiCallMaxWait, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error(e);
                throw new RuntimeException(e);
            }

            validateDuns(context);
            dnbMatchResultValidator.validate(context);
            DnBCache dnBCache = dnbCacheService.addCache(context);
            if (dnBCache != null) {
                context.setCacheId(dnBCache.getId());
            }
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
                                    "Found DnB match context in white cache: Name = %s, Country = %s, State = %s, City = %s, "
                                            + "ZipCode = %s, PhoneNumber = %s, DUNS = %s, ConfidenceCode = %d, MatchGrade = %s, "
                                            + "OutOfBusiness = %s, IsDunsInAM = %s, Duration = %d",
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
                                "Remove invalid white cache: Id=%s DUNS=%s OutOfBusiness=%s IsDunsInAM=%s",
                                cache.getId(), cache.getDuns(), cache.isOutOfBusinessString(),
                                cache.isDunsInAMString()));
                        dnbCacheService.removeCache(cache);
                    }
                } else {
                    context.copyResultFromCache(cache);
                    if (context.getLogDnBBulkResult()) {
                        log.info(String.format(
                                "Found DnB match context in black cache: Name = %s, Country = %s, State = %s, City = %s, "
                                        + "ZipCode = %s, PhoneNumber = %s, Duration = %d",
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
                if (unsubmittedBatches.isEmpty() || unsubmittedBatches.get(unsubmittedBatches.size() - 1).getContexts()
                        .size() == maximumBatchSize) {
                    DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
                    batchContext.setLogDnBBulkResult(context.getLogDnBBulkResult());
                    unsubmittedBatches.add(batchContext);
                }
                unsubmittedBatches.get(unsubmittedBatches.size() - 1).getContexts().put(context.getLookupRequestId(),
                        context);
            }
        } else {
            context.setDuration(System.currentTimeMillis() - request.getTimestamp());
            sendResponse(lookupRequestId, context, returnAddress);
        }
    }

    @Override
    public void bulkLookup(BulkLookupStrategy bulkLookupStrategy) {
        try {
            // Failed batch requests to process
            List<DnBBatchMatchContext> failedBatches = new ArrayList<>();
            // Success batch requests to process
            List<DnBBatchMatchContext> successBatches = new ArrayList<>();
            switch (bulkLookupStrategy) {
            case DISPATCHER:
                List<DnBBatchMatchContext> batchesToSubmit = new ArrayList<>();
                // Get the batches to submit (batch size = 10K or unsubmitted
                // record number is not changed)
                synchronized (unsubmittedBatches) {
                    int unsubmittedNum = getUnsubmittedRecordNum();
                    if (unsubmittedNum > 0) {
                        log.info(String.format("There are %d records unsubmitted before request dispatching",
                                unsubmittedNum));
                        if (unsubmittedNum >= maximumBatchSize || unsubmittedNum == previousUnsubmittedNum.get()) {
                            Iterator<DnBBatchMatchContext> iter = unsubmittedBatches.iterator();
                            while (iter.hasNext()) {
                                DnBBatchMatchContext batchContext = iter.next();
                                if (batchContext.getContexts().size() == maximumBatchSize
                                        || unsubmittedNum == previousUnsubmittedNum.get()) {
                                    batchesToSubmit.add(batchContext);
                                    iter.remove();
                                }
                            }
                        }
                    }
                }
                // Dispatch batch requests
                for (DnBBatchMatchContext batchContext : batchesToSubmit) {
                    try {
                        batchContext = dnbBulkLookupDispatcher.sendRequest(batchContext);
                    } catch (Exception ex) {
                        log.error(String.format("Exception in dispatching match requests to DnB bulk match service: %s",
                                ex.getMessage()));
                        batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
                    }
                    switch (batchContext.getDnbCode()) {
                    case SUBMITTED:
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
                int unsubmittedNum = getUnsubmittedRecordNum();
                if (unsubmittedNum > 0) {
                    log.info(String.format("There are %d records unsubmitted after request dispatching",
                            unsubmittedNum));
                }
                previousUnsubmittedNum.set(unsubmittedNum);
                // Process failed batch requests
                for (DnBBatchMatchContext batchContext : failedBatches) {
                    processBulkMatchResult(batchContext, false);
                }
                break;
            case STATUS:
                int pendingRecordNum = getUnsubmittedRecordNum() + getSubmittedRecordNum();
                // Batch requests to retry
                List<DnBBatchMatchContext> retryBatches = new ArrayList<>();
                synchronized (submittedBatches) {
                    dnbBulkLookupStatusChecker.checkStatus(submittedBatches);
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
                                if (pendingRecordNum <= bulkRetryPendingRecordThreshold) {
                                    retryBatches.add(submittedBatch);
                                } else {
                                    long mins = (System.currentTimeMillis() - submittedBatch.getTimestamp().getTime())
                                            / 60 / 1000;
                                    log.info(String.format(
                                            "Bulk match with serviceBatchId = %s was submitted %d minutes ago and hasn't got result back. Should retry. But currently there are already %d records unsubmitted or waiting for results. Will not resubmit for this time.",
                                            submittedBatch.getServiceBatchId(), mins, pendingRecordNum));
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
                    processBulkMatchResult(batchContext, false);
                }
                // Retry batch requests
                for (DnBBatchMatchContext retryBatch : retryBatches) {
                    long mins = (System.currentTimeMillis() - retryBatch.getTimestamp().getTime()) / 60 / 1000;
                    log.info(String.format(
                            "Bulk match with serviceBatchId = %s was submitted %d minutes ago and hasn't got result back. Resubmit it!",
                            retryBatch.getServiceBatchId(), mins));
                    retryBulkRequest(retryBatch);
                }
                break;
            case FETCHER:
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
                    processBulkMatchResult(batchContext, false);
                }
                // Process success batch requests
                for (DnBBatchMatchContext batchContext : successBatches) {
                    processBulkMatchResult(batchContext, true);
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "BulkLookupStrategy " + bulkLookupStrategy.name() + " is not supported in DnB bulk match");
            }
            if (submittedBatches.size() > 0) {
                log.info(String.format("There are %d batched requests waiting for DnB batch api to return results",
                        submittedBatches.size()));
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private int getUnsubmittedRecordNum() {
        if (unsubmittedBatches.isEmpty()) {
            return 0;
        }
        synchronized (unsubmittedBatches) {
            return maximumBatchSize * (unsubmittedBatches.size() > 1 ? unsubmittedBatches.size() - 1 : 0)
                    + unsubmittedBatches.get(unsubmittedBatches.size() - 1).getContexts().size();
        }
    }

    private int getSubmittedRecordNum() {
        int res = 0;
        synchronized (submittedBatches) {
            for (DnBBatchMatchContext batchContext : submittedBatches) {
                res += batchContext.getContexts().size();
            }
        }
        return res;
    }

    private void retryBulkRequest(DnBBatchMatchContext batchContext) {
        batchContext.setRetryTimes(batchContext.getRetryTimes() + 1);
        DnBBatchMatchContext retryContext = new DnBBatchMatchContext();
        retryContext.copyForRetry(batchContext);
        unsubmittedBatches.add(0, retryContext);
    }

    private void processBulkMatchResult(DnBBatchMatchContext batchContext, boolean success) {
        List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
        for (String lookupRequestId : batchContext.getContexts().keySet()) {
            String returnAddr = getReqReturnAddr(lookupRequestId);
            if (returnAddr == null) {
                log.info(String.format(
                        "Fail to find return address for lookup request %s. Bulk match request might be retried and response of this request has been sent. Give up!",
                        lookupRequestId));
                return;
            }
            DnBMatchContext context = batchContext.getContexts().get(lookupRequestId);
            if (!success) {
                context.setDnbCode(batchContext.getDnbCode());
            } else {
                validateDuns(context);
                dnbMatchResultValidator.validate(context);
                dnbCacheService.addCache(context);
            }
            removeReq(lookupRequestId);
            sendResponse(lookupRequestId, context, returnAddr);
            dnBMatchHistories.add(new DnBMatchHistory(context));
        }
        writeDnBMatchHistory(dnBMatchHistories);
    }

    /**
     * Called after DnB cache lookup, but before DnB remote lookup
     * If dunsInAM = true in cache, validate again:
     *      If still dunsInAM = true, adopt the cache
     *      If dunsInAM changed to false, reject the cache, go to remote DnB
     * If dunsInAM = false in cache, adopt the cache (result will be discarded in DnBMatchResultValidator)
     */
    private boolean adoptWhiteCache(DnBCache cache, String dataCloudVersion) {
        if (Boolean.TRUE.equals(cache.isOutOfBusiness()) || Boolean.FALSE.equals(cache.isDunsInAM())) {
            cache.setDunsInAM(Boolean.FALSE);
            return true;
        }
        if (isDunsInAM(cache.getDuns(), dataCloudVersion)) {
            cache.setDunsInAM(Boolean.TRUE);
            return true;
        } else {
            cache.setDunsInAM(Boolean.FALSE);
            return false;
        }
    }

    /**
     * Called after DnB remote lookup
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
                    log.error(ex);
                }
                return returnedContext;
            }
        };
        return task;
    }
}
