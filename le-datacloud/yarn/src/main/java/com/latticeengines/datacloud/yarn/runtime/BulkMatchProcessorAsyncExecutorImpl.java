package com.latticeengines.datacloud.yarn.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.exposed.service.MatchMonitorService;
import com.latticeengines.datacloud.match.service.DnbMatchCommandService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

import scala.concurrent.Future;

@Component("bulkMatchProcessorAsyncExecutor")
public class BulkMatchProcessorAsyncExecutorImpl extends AbstractBulkMatchProcessorExecutorImpl {

    private static final int NUM_10K = 10_000;
    private static final int NUM_1K = 1_000;
    private static final Logger log = LoggerFactory.getLogger(BulkMatchProcessorAsyncExecutorImpl.class);

    @Value("${datacloud.dnb.quota.check.disabled}")
    private boolean disableDnBCheck;

    @Autowired
    private RateLimitingService rateLimitingService;

    @Autowired
    private MatchMonitorService matchMonitorService;
    @Autowired
    private DnbMatchCommandService dnbMatchCommandService;

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
            if (combinedContext == null) {
                combinedContext = matchContext;
            }
            if (processorContext.isPartialMatch()) {
                processPartialMatch(processorContext, internalRecords, internalCompletedRecords, futures,
                        completedFutures, matchContext, combinedContext);
                block++;
                continue;
            }
            matchContext = matchExecutor.executeAsync(matchContext);
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
            if (processorContext.isPartialMatch()) {
                processPartialMatch(processorContext, internalRecords, internalCompletedRecords, futures,
                        completedFutures, null, combinedContext);
                continue;
            }
            processRecords(processorContext, startTime, internalRecords, internalCompletedRecords, futures,
                    completedFutures, combinedContext, 0, 0);
        }

        // added for cleanup
        dnbMatchCommandService.finalize(processorContext.getRootOperationUid());
        log.info(String.format("Finished matching %d rows in %.2f minutes.", processorContext.getBlockSize(),
                (System.currentTimeMillis() - startTime) / 60000.0));

        matchMonitorService.monitor();
    }

    private void processPartialMatch(ProcessorContext processorContext, List<InternalOutputRecord> internalRecords,
            List<InternalOutputRecord> internalCompletedRecords, List<Future<Object>> futures,
            List<Future<Object>> completedFutures, MatchContext matchContext, MatchContext combinedContext) {
        if (internalCompletedRecords.size() > 0)
            flushMatchOutput(processorContext, internalCompletedRecords, completedFutures, combinedContext);
        if (internalRecords.size() > 0)
            flushMatchOutput(processorContext, internalRecords, futures, combinedContext);
        if (matchContext != null)
            flushMatchOutput(processorContext, matchContext.getInternalResults(), new ArrayList<Future<Object>>(),
                    combinedContext);
    }

    private void flushMatchOutput(ProcessorContext processorContext, List<InternalOutputRecord> records,
            List<Future<Object>> futures, MatchContext combinedContext) {
        combinedContext.setInternalResults(records);
        combinedContext.setFuturesResult(futures);
        log.info("Flushing " + records.size() + " records.");

        populateMatchOutput(processorContext, records, combinedContext);
        processorContext.getRowsProcessed().addAndGet(records.size());
        processMatchOutput(processorContext, combinedContext.getOutput());
        int rows = processorContext.getRowsProcessed().get();
        processorContext.getDataCloudProcessor().setProgress(0.07f + 0.9f * rows / processorContext.getBlockSize());
        log.info("Processed " + rows + " out of " + processorContext.getBlockSize() + " rows.");
        futures.clear();
        records.clear();
    }

    private void populateMatchOutput(ProcessorContext processorContext, List<InternalOutputRecord> records,
            MatchContext combinedContext) {
        MatchOutput matchOutput = combinedContext.getOutput();
        List<OutputRecord> outputRecords = new ArrayList<>();
        for (InternalOutputRecord record : records) {
            OutputRecord outputRecord = new OutputRecord();
            outputRecords.add(outputRecord);
            List<Object> output = new ArrayList<>(Collections.nCopies(matchOutput.getOutputFields().size(), null));
            outputRecord.setOutput(output);
            outputRecord.setInput(record.getInput());
        }
        matchOutput.setResult(outputRecords);

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
                    completedFutures, combinedContext, 100, NUM_10K * 4);
            if (processorContext.isPartialMatch()) {
                return;
            }
        } while (futures.size() > NUM_10K * 5);
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
            if (processorContext.getOriginalInput().isPartialMatchEnabled()) {
                processorContext.setPartialMatch(true);
                log.warn("Set partial match!");
                return;
            }
            throw new RuntimeException(String.format("Did not finish matching %d rows in %.2f minutes.",
                    processorContext.getBlockSize(), processorContext.getTimeOut() / 60000.0));
        }
    }

    private void sleepSeconds(int count) {
        try {
            Thread.sleep(1_000L);
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
            InternalOutputRecord record = internalRecords.get(i);
            if (future == null) {
                if (isAttrLookup(processorContext) && record.isMatched()) {
                    // can already found custom account during pre-lookup by account ID
                    currentCompletedfutures.add(null);
                    currentCompletedRecords.add(record);
                } else if (CollectionUtils.isNotEmpty(record.getErrorMessages())) {
                    log.warn("There's match input errors=" + String.join("|", record.getErrorMessages()));
                } else {
                    log.warn("Match future is null for unknown reason!");
                }
                continue;
            }
            if (future.isCompleted()) {
                currentCompletedfutures.add(future);
                currentCompletedRecords.add(record);
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

    private boolean isAttrLookup(ProcessorContext ctx) {
        if (ctx == null || ctx.getOriginalInput() == null) {
            return false;
        }

        // check only entity match attr lookup for now, add cdl lookup later if
        // necessary
        return OperationalMode.ENTITY_MATCH_ATTR_LOOKUP.equals(ctx.getOriginalInput().getOperationalMode());
    }

    private void checkIfProceed(ProcessorContext processorContext) {
        if (!processorContext.getDivider().hasNextGroup() || !processorContext.isUseRemoteDnB() || disableDnBCheck) {
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
