package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
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

    private final Queue<DnBMatchContext> comingContexts = new ConcurrentLinkedQueue<>();
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> unsubmittedReqs = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> submittedReqs = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> finishedReqs = Collections.synchronizedList(new ArrayList<>());
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
            comingContexts.offer(context);
        } else {
            context.setDuration(System.currentTimeMillis() - request.getTimestamp());
            sendResponse(lookupRequestId, context, returnAddress);
        }
    }

    @Override
    public void bulkLookup(BulkLookupStrategy bulkLookupStrategy) {
        try {
            switch (bulkLookupStrategy) {
            case DISPATCHER:
                synchronized (unsubmittedReqs) {
                    synchronized (comingContexts) {
                        while (!comingContexts.isEmpty()) {
                            DnBMatchContext context = comingContexts.poll();
                            if (unsubmittedReqs.isEmpty() || unsubmittedReqs.get(unsubmittedReqs.size() - 1)
                                    .getContexts().size() == maximumBatchSize) {
                                DnBBatchMatchContext batchContext = new DnBBatchMatchContext();
                                batchContext.setLogDnBBulkResult(context.getLogDnBBulkResult());
                                unsubmittedReqs.add(batchContext);
                            }
                            unsubmittedReqs.get(unsubmittedReqs.size() - 1).getContexts()
                                    .put(context.getLookupRequestId(), context);
                        }
                    }
                    List<DnBBatchMatchContext> batchContexts = new ArrayList<DnBBatchMatchContext>();
                    int unsubmittedNum = getUnsubmittedNum();
                    if (unsubmittedNum > 0) {
                        log.info(String.format("There are %d requests unsubmitted before request dispatching",
                                unsubmittedNum));
                    }
                    if (unsubmittedNum >= maximumBatchSize || unsubmittedNum == previousUnsubmittedNum.get()) {
                        Iterator<DnBBatchMatchContext> iter = unsubmittedReqs.iterator();
                        while (iter.hasNext()) {
                            DnBBatchMatchContext batchContext = iter.next();
                            if (batchContext.getContexts().size() == maximumBatchSize
                                    || unsubmittedNum == previousUnsubmittedNum.get()) {
                                batchContexts.add(batchContext);
                                iter.remove();
                            }
                        }
                    }
                    if (!batchContexts.isEmpty()) {
                        int num = unsubmittedReqs.size();
                        for (DnBBatchMatchContext batchContext : batchContexts) {
                            try {
                                batchContext = dnbBulkLookupDispatcher.sendRequest(batchContext);
                            } catch (Exception ex) {
                                log.error(String.format(
                                        "Exception in dispatching match requests to DnB bulk match service: %s",
                                        ex.getMessage()));
                                batchContext.setDnbCode(DnBReturnCode.UNKNOWN);
                            }
                            switch (batchContext.getDnbCode()) {
                            case SUBMITTED:
                                submittedReqs.add(batchContext);
                                break;
                            case RATE_LIMITING:
                            case EXCEED_LIMIT_OR_UNAUTHORIZED:
                                // Not allow to submit this request due to rate limiting
                                // Put it back to unsubmittedReqs list. Maintain the same order in the unsubmittedReqs
                                unsubmittedReqs.add(unsubmittedReqs.size() - num, batchContext);
                                break;
                            default:
                                processBulkMatchResult(batchContext, false);
                                break;
                            }
                        }
                    }
                    unsubmittedNum = getUnsubmittedNum();
                    if (unsubmittedNum > 0) {
                        log.info(String.format("There are %d requests unsubmitted after request dispatching",
                                unsubmittedNum));
                    }
                    previousUnsubmittedNum.set(unsubmittedNum);
                }
                break;
            case STATUS:
                synchronized (submittedReqs) {
                    dnbBulkLookupStatusChecker.checkStatus(submittedReqs);
                    Iterator<DnBBatchMatchContext> iter = submittedReqs.iterator();
                    while (iter.hasNext()) {
                        DnBBatchMatchContext submittedReq = iter.next();
                        switch (submittedReq.getDnbCode()) {
                        case OK:
                            finishedReqs.add(submittedReq);
                            iter.remove();
                            break;
                        case SUBMITTED:
                        case IN_PROGRESS:
                        case RATE_LIMITING:
                        case EXCEED_LIMIT_OR_UNAUTHORIZED:
                            if (submittedReq.getRetryTimes() < bulkRetryTimes && submittedReq.getTimestamp() != null
                                    && (System.currentTimeMillis()
                                            - submittedReq.getTimestamp().getTime() >= bulkRetryWait)) {
                                long mins = (System.currentTimeMillis() - submittedReq.getTimestamp().getTime()) / 60
                                        / 1000;
                                log.info(String.format(
                                        "Bulk match with serviceBatchId = %s was submitted %d minutes ago and hasn't got result back. Resubmit it!",
                                        submittedReq.getServiceBatchId(), mins));
                                submittedReq.setRetryTimes(submittedReq.getRetryTimes() + 1);
                                retryBulkRequest(submittedReq);
                            }
                            break;
                        default:
                            processBulkMatchResult(submittedReq, false);
                            iter.remove();
                            break;
                        }
                    }
                }
                break;
            case FETCHER:
                synchronized (finishedReqs) {
                    Iterator<DnBBatchMatchContext> iter = finishedReqs.iterator();
                    while (iter.hasNext()) {
                        DnBBatchMatchContext finishedReq = iter.next();
                        try {
                            finishedReq = dnbBulkLookupFetcher.getResult(finishedReq);
                        } catch (Exception ex) {
                            log.error(String.format(
                                    "Fail to poll match result for request %s from DnB bulk matchc service: %s",
                                    finishedReq.getServiceBatchId(), ex.getMessage()));
                            finishedReq.setDnbCode(DnBReturnCode.UNKNOWN);
                        }
                        switch (finishedReq.getDnbCode()) {
                        case OK:
                            processBulkMatchResult(finishedReq, true);
                            iter.remove();
                            break;
                        default:
                            processBulkMatchResult(finishedReq, false);
                            iter.remove();
                        }
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "BulkLookupStrategy " + bulkLookupStrategy.name() + " is not supported in DnB bulk match");
            }
            if (submittedReqs.size() > 0) {
                log.info(String.format("There are %d batched requests waiting for DnB batch api to return results",
                        submittedReqs.size()));
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private void retryBulkRequest(DnBBatchMatchContext batchContext) {
        DnBBatchMatchContext retryContext = new DnBBatchMatchContext();
        retryContext.copyForRetry(batchContext);
        unsubmittedReqs.add(0, retryContext);
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

    private int getUnsubmittedNum() {
        if (unsubmittedReqs.isEmpty()) {
            return 0;
        }
        synchronized (unsubmittedReqs) {
            return maximumBatchSize * (unsubmittedReqs.size() > 1 ? unsubmittedReqs.size() - 1 : 0)
                    + unsubmittedReqs.get(unsubmittedReqs.size() - 1).getContexts().size();
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
