package com.latticeengines.datacloud.yarn.runtime;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import scala.concurrent.Future;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

@Component("bulkMatchProcessorAsyncExecutor")
public class BulkMatchProcessorAsyncExecutorImpl extends AbstractBulkMatchProcessorExecutorImpl {

    private static final int NUM_10K = 10_000;
    private static final Log log = LogFactory.getLog(BulkMatchProcessorAsyncExecutorImpl.class);

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
        while (processorContext.getDivider().hasNextGroup()) {
            MatchInput input = constructMatchInputFromData(processorContext);
            // cache an input to generate output metric
            if (processorContext.getMatchInput() == null) {
                MatchInput matchInput = JsonUtils.deserialize(JsonUtils.serialize(input), MatchInput.class);
                processorContext.setMatchInput(matchInput);
            }

            log.info("s block " + block + " of " + input.getData().size() + " records to Async match.");
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
                    completedFutures, combinedContext, 2_000, NUM_10K * 2);
        } while (futures.size() > NUM_10K * 4);
    }

    private void processRecords(ProcessorContext processorContext, Long startTime,
            List<InternalOutputRecord> internalRecords, List<InternalOutputRecord> internalCompletedRecords,
            List<Future<Object>> futures, List<Future<Object>> completedFutures, MatchContext combinedContext,
            int completedThreshold, int waitThreshold) {

        getCompletedRecords(processorContext, internalRecords, internalCompletedRecords, futures, completedFutures);
        if (completedFutures.size() > completedThreshold) {
            consumeFutures(processorContext, internalCompletedRecords, completedFutures, combinedContext);
        }
        if (futures.size() > waitThreshold) {
            sleepSeconds(futures.size());
        }
        checkTimeout(processorContext, startTime);
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
            log.warn("Failed to sleep!", ex);
        }
    }

    private void consumeFutures(ProcessorContext processorContext, List<InternalOutputRecord> internalCompletedRecords,
            List<Future<Object>> completedFutures, MatchContext combinedContext) {
        if (CollectionUtils.isEmpty(completedFutures) || CollectionUtils.isEmpty(internalCompletedRecords)) {
            log.warn("There's no input or future records!");
            return;
        }
        if (internalCompletedRecords.size() != completedFutures.size()) {
            String msg = "Input records and future records do have the same number!";
            log.error(msg);
            throw new RuntimeException(msg);
        }
        combinedContext.setInternalResults(internalCompletedRecords);
        combinedContext.setFuturesResult(completedFutures);
        log.info("Sending " + completedFutures.size() + " of futures to get results.");

        combinedContext = matchExecutor.executeMatchResult(combinedContext);
        log.info("Returned " + combinedContext.getOutput().getResult().size() + " of record results.");
        processorContext.getRowsProcessed().addAndGet(internalCompletedRecords.size());

        if (combinedContext != null && !combinedContext.getOutput().getResult().isEmpty()) {
            processMatchOutput(processorContext, combinedContext.getOutput());
            int rows = processorContext.getRowsProcessed().get();
            processorContext.getDataCloudProcessor().setProgress(0.07f + 0.9f * rows / processorContext.getBlockSize());
            log.info("Processed " + rows + " out of " + processorContext.getBlockSize() + " rows.");
        }

        completedFutures.clear();
        internalCompletedRecords.clear();
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
    }

}
