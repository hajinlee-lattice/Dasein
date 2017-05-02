package com.latticeengines.datacloud.yarn.runtime;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.exposed.service.MatchMonitorService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

import scala.concurrent.Future;

@Component("bulkMatchProcessorAsyncExecutor")
public class BulkMatchProcessorAsyncExecutorImpl extends AbstractBulkMatchProcessorExecutorImpl {

    private static final int NUM_10K = 10_000;
    private static final int NUM_1K = 1_000;
    private static final Log log = LogFactory.getLog(BulkMatchProcessorAsyncExecutorImpl.class);

    @Autowired
    private RateLimitingService rateLimitingService;

    @Autowired
    private MatchMonitorService matchMonitorService;
    

    @Override
    public void execute(ProcessorContext processorContext) {

        processorContext.getDataCloudProcessor().setProgress(0.07f);
        Long startTime = System.currentTimeMillis();
        List<Future<Object>> futures = new ArrayList<>();
        List<Future<Object>> completedFutures = new ArrayList<>();
        List<InternalOutputRecord> internalRecords = new ArrayList<>();
        List<InternalOutputRecord> internalCompletedRecords = new ArrayList<>();
        MatchContext combinedContext = null;
        int block = 0;
        checkIfProceed(processorContext);
        while (processorContext.getDivider().hasNextGroup()) {
            MatchInput input = constructMatchInputFromData(processorContext);
            // cache an input to generate output metric
            if (processorContext.getGroupMatchInput() == null) {
                MatchInput matchInput = JsonUtils.deserialize(JsonUtils.serialize(input), MatchInput.class);
                processorContext.setGroupMatchInput(matchInput);
            }

            log.info("a block " + block + " of " + input.getData().size() + " records to Async match.");
            MatchContext matchContext = matchPlanner.plan(input);
            matchContext = matchExecutor.executeAsync(matchContext);
            if (combinedContext == null) {
                combinedContext = matchContext;
            }

            if (CollectionUtils.isNotEmpty(matchContext.getFuturesResult())) {
                log.info("Returned block " + block + " of " + matchContext.getFuturesResult().size()
                        + " futures from Async match.");
            }
            processFutures(processorContext, startTime, internalRecords, internalCompletedRecords, futures,
                    completedFutures, matchContext, combinedContext);
            block++;
        }

        while (futures.size() > 0 || completedFutures.size() > 0) {
            log.info("Last batch, futures size=" + futures.size() + " completed futures size="
                    + completedFutures.size());
            processRecords(processorContext, startTime, internalRecords, internalCompletedRecords, futures,
                    completedFutures, combinedContext, 0, 0);
        }

        log.info(String.format("Finished matching %d rows in %.2f minutes.", processorContext.getBlockSize(),
                (System.currentTimeMillis() - startTime) / 60000.0));

        matchMonitorService.monitor();
    }

    private void processFutures(ProcessorContext processorContext, Long startTime,
            List<InternalOutputRecord> internalRecords, List<InternalOutputRecord> internalCompletedRecords,
            List<Future<Object>> futures, List<Future<Object>> completedFutures, MatchContext matchContext,
            MatchContext combinedContext) {
        if (CollectionUtils.isEmpty(matchContext.getFuturesResult())
                || CollectionUtils.isEmpty(matchContext.getInternalResults())) {
            log.warn("There's no matched records!");
            return;
        }
        internalRecords.addAll(matchContext.getInternalResults());
        futures.addAll(matchContext.getFuturesResult());

        do {
            processRecords(processorContext, startTime, internalRecords, internalCompletedRecords, futures,
                    completedFutures, combinedContext, 100, NUM_10K * 2);
        } while (futures.size() > NUM_10K * 4);
    }

    private void processRecords(ProcessorContext processorContext, Long startTime,
            List<InternalOutputRecord> internalRecords, List<InternalOutputRecord> internalCompletedRecords,
            List<Future<Object>> futures, List<Future<Object>> completedFutures, MatchContext combinedContext,
            int completedThreshold, int waitThreshold) {

        getCompletedRecords(processorContext, internalRecords, internalCompletedRecords, futures, completedFutures);
        if (completedFutures.size() >= completedThreshold) {
            consumeFutures(processorContext, internalCompletedRecords, completedFutures, combinedContext);
        }
        if (futures.size() > waitThreshold) {
            sleepSeconds(futures.size());
        }
        checkTimeout(processorContext, startTime);
    }

    private void consumeFutures(ProcessorContext processorContext, List<InternalOutputRecord> internalCompletedRecords,
            List<Future<Object>> completedFutures, MatchContext combinedContext) {
        List<InternalOutputRecord> batchCompletedRecords = new ArrayList<>();
        List<Future<Object>> batchCompletedFutures = new ArrayList<>();
        for (int i = 0; i < completedFutures.size(); i++) {
            batchCompletedRecords.add(internalCompletedRecords.get(i));
            batchCompletedFutures.add(completedFutures.get(i));
            if (batchCompletedFutures.size() >= NUM_1K) {
                consumeBatchFutures(processorContext, batchCompletedRecords, batchCompletedFutures, combinedContext);
            }
        }
        if (batchCompletedFutures.size() > 0) {
            consumeBatchFutures(processorContext, batchCompletedRecords, batchCompletedFutures, combinedContext);
        }
        internalCompletedRecords.clear();
        completedFutures.clear();
    }

    private void checkTimeout(ProcessorContext processorContext, Long startTime) {
        if (System.currentTimeMillis() - startTime > processorContext.getTimeOut()) {
            throw new RuntimeException(String.format("Did not finish matching %d rows in %.2f minutes.",
                    processorContext.getBlockSize(), processorContext.getTimeOut() / 60000.0));
        }
    }

    private void sleepSeconds(int count) {
        try {
            log.info("Many records, sleep for seconds! size=" + count);
            Thread.sleep(10_000L);
        } catch (Exception ex) {
            log.warn("Failed to sleep!");
        }
    }

    private void consumeBatchFutures(ProcessorContext processorContext,
            List<InternalOutputRecord> batchCompletedRecords, List<Future<Object>> batchCompletedFutures,
            MatchContext combinedContext) {
        if (CollectionUtils.isEmpty(batchCompletedFutures) || CollectionUtils.isEmpty(batchCompletedRecords)) {
            log.warn("There's no input or future records!");
            return;
        }
        if (batchCompletedRecords.size() != batchCompletedFutures.size()) {
            String msg = "Input records and future records do have the same number!";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        combinedContext.setInternalResults(batchCompletedRecords);
        combinedContext.setFuturesResult(batchCompletedFutures);
        log.info("Sending " + batchCompletedFutures.size() + " of futures to get results.");

        combinedContext = matchExecutor.executeMatchResult(combinedContext);
        log.info("Returned " + combinedContext.getOutput().getResult().size() + " of record results.");
        processorContext.getRowsProcessed().addAndGet(batchCompletedRecords.size());
        checkMatchCode(processorContext, combinedContext);

        processMatchOutput(processorContext, combinedContext.getOutput());
        int rows = processorContext.getRowsProcessed().get();
        processorContext.getDataCloudProcessor().setProgress(0.07f + 0.9f * rows / processorContext.getBlockSize());
        log.info("Processed " + rows + " out of " + processorContext.getBlockSize() + " rows.");

        batchCompletedFutures.clear();
        batchCompletedRecords.clear();
    }

    private void checkMatchCode(ProcessorContext processorContext, MatchContext combinedContext) {
        for (InternalOutputRecord outputRecord : combinedContext.getInternalResults()) {
            if (DnBReturnCode.UNMATCH_TIMEOUT.equals(outputRecord.getDnbCode())) {
                throw new RuntimeException(String.format(
                        "Did not finish matching in %.2f minutes due to DnB limits, please try later!",
                        processorContext.getRecordTimeOut() / 60000.0));
            }
        }
    }

    private void getCompletedRecords(ProcessorContext processorContext, List<InternalOutputRecord> internalRecords,
            List<InternalOutputRecord> internalCompletedRecords, List<Future<Object>> futures,
            List<Future<Object>> completedFutures) {

        List<Future<Object>> currentCompletedfutures = new ArrayList<>();
        List<InternalOutputRecord> currentCompletedRecords = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            Future<Object> future = futures.get(i);
            InternalOutputRecord internRecord = internalRecords.get(i);
            if (future.isCompleted()) {
                currentCompletedfutures.add(future);
                currentCompletedRecords.add(internRecord);
            }
        }
        if (currentCompletedfutures.size() > 0) {
            completedFutures.addAll(currentCompletedfutures);
            futures.removeAll(currentCompletedfutures);
        }
        if (currentCompletedRecords.size() > 0) {
            internalCompletedRecords.addAll(currentCompletedRecords);
            internalRecords.removeAll(currentCompletedRecords);
        }
        matchMonitorService.pushMetrics("FUTURES", String.format("futures in actor system: %d", futures.size()));
    }

    private void checkIfProceed(ProcessorContext processorContext) {
        if (!processorContext.getDivider().hasNextGroup() || !processorContext.isUseRemoteDnB()) {
            return;
        }
        long timeout = (long) (processorContext.getTimeOut() * 0.75);
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeout) {
            if (isDnBQuotaAvaiable()) {
                return;
            }
            try {
                log.info(String
                        .format("DnB bulk match quota is not available. Wait for 4 mins. Have waited for %d mins up to now. Plan to wait for %d mins for total.",
                                (System.currentTimeMillis() - startTime) / 60000, timeout / 60000));
                Thread.sleep(240_000L);
            } catch (Exception ex) {
                log.warn("Failed to sleep!");
            }
        }
        throw new RuntimeException("DnB bulk match is congested. Fail the bulk match job!");
    }

    private boolean isDnBQuotaAvaiable() {
        for (int i = 0; i < 4; i++) {
            RateLimitedAcquisition rlAcq = rateLimitingService.acquireDnBBulkRequest(1, true);
            if (!rlAcq.isAllowed()) {
                logRateLimitingRejection(rlAcq);
                return false;
            }
            if (i == 3) {
                return true;
            }
            try {
                log.info(String.format(
                        "Wait for 15 seconds to ensure DnB bulk match quota is available. Try times: %d", i + 1));
                Thread.sleep(15_000L);
            } catch (Exception ex) {
                log.warn("Failed to sleep!");
            }
        }
        return true;
    }

    protected void logRateLimitingRejection(RateLimitedAcquisition rlAcq) {
        StringBuilder sb1 = new StringBuilder();
        if (rlAcq.getRejectionReasons() != null) {
            for (String rejectionReason : rlAcq.getRejectionReasons()) {
                sb1.append(rejectionReason + " ");
            }
        }
        StringBuilder sb2 = new StringBuilder();
        if (rlAcq.getExceedingQuotas() != null) {
            for (String exceedingQuota : rlAcq.getExceedingQuotas()) {
                sb2.append(exceedingQuota + " ");
            }
        }
        log.info("DnB bulk match is congested. Rejection reasons: %s   Exceeding quotas: %s");
    }

}
