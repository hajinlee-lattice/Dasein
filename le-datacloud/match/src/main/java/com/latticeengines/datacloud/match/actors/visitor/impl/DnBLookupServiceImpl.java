package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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

import com.latticeengines.datacloud.match.actors.visitor.BulkLookupStrategy;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.dnb.DnBBatchMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBCache;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
import com.latticeengines.datacloud.match.service.DnBBulkLookupDispatcher;
import com.latticeengines.datacloud.match.service.DnBBulkLookupFetcher;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.datacloud.match.service.DnBRealTimeLookupService;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("dnBLookupService")
public class DnBLookupServiceImpl extends DataSourceLookupServiceBase {
    private static final Log log = LogFactory.getLog(DnBLookupServiceImpl.class);

    @Value("${datacloud.dnb.bulk.request.expire.duration.minute}")
    private int requestExpireDuration;

    @Value("${datacloud.dnb.bulk.request.maximum}")
    private int maximumBatchSize;

    @Value("${datacloud.match.actor.datasource.dnb.threadpool.count.min}")
    private Integer dnbThreadpoolCountMin;

    @Value("${datacloud.match.actor.datasource.dnb.threadpool.count.max}")
    private Integer dnbThreadpoolCountMax;

    @Value("${datacloud.match.actor.datasource.dnb.api.call.maxwait}")
    private Integer dnbApiCallMaxWait;

    @Autowired
    private DnBRealTimeLookupService dnbRealTimeLookupService;

    @Autowired
    private DnBBulkLookupDispatcher dnbBulkLookupDispatcher;

    @Autowired
    private DnBBulkLookupFetcher dnbBulkLookupFetcher;

    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

    private ExecutorService dnbDataSourceServiceExecutor;

    private final Queue<DnBMatchContext> comingContexts = new ConcurrentLinkedQueue<>();
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> unsubmittedReqs = Collections.synchronizedList(new ArrayList<>());
    @SuppressWarnings("unchecked")
    private final List<DnBBatchMatchContext> submittedReqs = Collections.synchronizedList(new ArrayList<>());

    private static AtomicInteger previousUnsubmittedNum = new AtomicInteger(0);

    @PostConstruct
    public void postConstruct() {
        initDnBDataSourceThreadPool();
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
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        MatchTraveler traveler = request.getMatchTravelerContext();
        if (traveler.isUseDnBCache()) {
            Long startTime = System.currentTimeMillis();
            DnBCache cache = dnbCacheService.lookupCache(context);
            if (cache != null) {
                if (cache.isWhiteCache()) {
                    if (isValidDuns(cache.getDuns(), traveler.getDataCloudVersion())) {
                        context.copyResultFromCache(cache);
                        dnbMatchResultValidator.validate(context);
                        context.setDuration(System.currentTimeMillis() - startTime);
                        if (log.isDebugEnabled()) {
                            log.debug(String.format(
                                    "Found DnB match context for request %s in white cache: Status = %s, Duration = %d",
                                    context.getLookupRequestId(), context.getDnbCode().getMessage(),
                                    context.getDuration()));
                        }
                        return context;
                    } else {
                        log.info("Remove invalid white cache: Id= " + cache.getId() + " DUNS=" + cache.getDuns());
                        dnbCacheService.removeCache(cache);
                    }
                } else {
                    context.copyResultFromCache(cache);
                    context.setDuration(System.currentTimeMillis() - startTime);
                    if (log.isDebugEnabled()) {
                        log.debug(String.format(
                                "Found DnB match context for request %s in black cache: Status = %s, Duration = %d",
                                context.getLookupRequestId(), context.getDnbCode().getMessage(),
                                context.getDuration()));
                    }
                    return context;
                }

            }
        }
        if (traveler.isUseRemoteDnB()) {
            Callable<DnBMatchContext> task = createCallableForRemoteDnBApiCall(context);
            Future<DnBMatchContext> dnbFuture = dnbDataSourceServiceExecutor.submit(task);

            try {
                context = dnbFuture.get(dnbApiCallMaxWait, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error(e);
                throw new RuntimeException(e);
            }

            dnbCacheService.addCache(context);
            List<DnBMatchHistory> dnBMatchHistories = new ArrayList<>();
            dnBMatchHistories.add(new DnBMatchHistory(context));
            writeDnBMatchHistory(dnBMatchHistories);
        }
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
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
        MatchTraveler traveler = request.getMatchTravelerContext();
        context.setLogDnBBulkResult(traveler.getLogDnBBulkResult());
        if (traveler.isUseDnBCache()) {
            Long startTime = System.currentTimeMillis();
            DnBCache cache = dnbCacheService.lookupCache(context);
            if (cache != null) {
                if (cache.isWhiteCache()) {
                    if (isValidDuns(cache.getDuns(), traveler.getDataCloudVersion())) {
                        context.copyResultFromCache(cache);
                        dnbMatchResultValidator.validate(context);
                        context.setDuration(System.currentTimeMillis() - startTime);
                        sendResponse(lookupRequestId, context, returnAddress);
                        if (log.isDebugEnabled()) {
                            log.debug(String.format(
                                    "Found DnB match context for request %s in white cache: Status = %s, Duration = %d",
                                    context.getLookupRequestId(), context.getDnbCode().getMessage(),
                                    context.getDuration()));
                        } else if (context.getLogDnBBulkResult()) {
                            log.info(String.format(
                                    "Found DnB match context in white cache: Name = %s, Country = %s, State = %s, City = %s, "
                                            + "ZipCode = %s, PhoneNumber = %s, DUNS = %s, ConfidenceCode = %d, MatchGrade = %s",
                                    context.getInputNameLocation().getName(),
                                    context.getInputNameLocation().getCountry(),
                                    context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                                    context.getInputNameLocation().getZipcode(),
                                    context.getInputNameLocation().getPhoneNumber(), context.getDuns(),
                                    context.getConfidenceCode(), context.getMatchGrade().getRawCode()));
                        }
                        return;
                    } else {
                        log.info("Remove invalid white cache: Id= " + cache.getId() + " DUNS=" + cache.getDuns());
                        dnbCacheService.removeCache(cache);
                    }
                } else {
                    context.copyResultFromCache(cache);
                    context.setDuration(System.currentTimeMillis() - startTime);
                    sendResponse(lookupRequestId, context, returnAddress);
                    if (log.isDebugEnabled()) {
                        log.debug(String.format(
                                "Found DnB match context for request %s in black cache: Status = %s, Duration = %d",
                                context.getLookupRequestId(), context.getDnbCode().getMessage(),
                                context.getDuration()));
                    } else if (context.getLogDnBBulkResult()) {
                        log.info(String.format(
                                "Found DnB match context in black cache: Name = %s, Country = %s, State = %s, City = %s, "
                                        + "ZipCode = %s, PhoneNumber = %s",
                                context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                                context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                                context.getInputNameLocation().getZipcode(),
                                context.getInputNameLocation().getPhoneNumber()));
                    }
                    return;
                }

            }
        }

        if (traveler.isUseRemoteDnB()) {
            saveReq(lookupRequestId, returnAddress, request);
            comingContexts.offer(context);
            if (log.isDebugEnabled()) {
                log.debug("Accepted request " + context.getLookupRequestId());
            }
        } else {
            Long startTime = System.currentTimeMillis();
            context.setDuration(System.currentTimeMillis() - startTime);
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
                            case OK:
                                submittedReqs.add(batchContext);
                                break;
                            case UNSUBMITTED:
                                // Too many requests are waiting for results.
                                // This request is not submitted.
                                // Put it back to unsubmittedReqs list. Maintain
                                // the same order in the unsubmittedReqs
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
            case FETCHER:
                synchronized (submittedReqs) {
                    Iterator<DnBBatchMatchContext> iter = submittedReqs.iterator();
                    while (iter.hasNext()) {
                        DnBBatchMatchContext submittedReq = iter.next();
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
                            iter.remove();
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
                                iter.remove();
                            }
                        } else {
                            processBulkMatchResult(submittedReq, false);
                            iter.remove();
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
            if (submittedReqs.size() > 0) {
                log.info(String.format("There are %d batched requests waiting for DnB batch api to return results",
                        submittedReqs.size()));
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
            String returnAddr = getReqReturnAddr(lookupRequestId);
            removeReq(lookupRequestId);
            sendResponse(lookupRequestId, context, returnAddr);
            dnBMatchHistories.add(new DnBMatchHistory(context));
        }
        writeDnBMatchHistory(dnBMatchHistories);
    }

    private boolean isValidDuns(String duns, String dataCloudVersion) {
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
