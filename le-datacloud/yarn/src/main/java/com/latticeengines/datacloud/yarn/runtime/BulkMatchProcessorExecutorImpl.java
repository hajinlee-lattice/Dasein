package com.latticeengines.datacloud.yarn.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

@Component("bulkMatchProcessorExecutor")
public class BulkMatchProcessorExecutorImpl extends AbstractBulkMatchProcessorExecutorImpl {

    private static final Log log = LogFactory.getLog(BulkMatchProcessorExecutorImpl.class);

    @Override
    public void execute(ProcessorContext processorContext) {

        ExecutorService executor = Executors.newFixedThreadPool(processorContext.getNumThreads());
        processorContext.getDataCloudProcessor().setProgress(0.07f);
        Long startTime = System.currentTimeMillis();
        Set<Future<MatchContext>> futures = new HashSet<>();
        while (processorContext.getDivider().hasNextGroup()) {
            if (futures.size() < processorContext.getNumThreads()) {
                // create new input object for each record
                MatchInput input = constructMatchInputFromData(processorContext);
                // cache an input to generate output metric
                if (processorContext.getMatchInput() == null) {
                    MatchInput matchInput = JsonUtils.deserialize(JsonUtils.serialize(input), MatchInput.class);
                    processorContext.setMatchInput(matchInput);
                }
                Future<MatchContext> future;
                if (processorContext.isUseProxy()) {
                    future = executor.submit(new RealTimeMatchCallable(input, processorContext.getPodId(), matchProxy));
                } else {
                    future = executor.submit(new BulkMatchCallable(input, processorContext.getPodId(), matchPlanner,
                            matchExecutor));
                }
                futures.add(future);
            }
            if (futures.size() >= processorContext.getNumThreads()) {
                consumeFutures(processorContext, futures);
            }
        }

        while (!futures.isEmpty()) {
            consumeFutures(processorContext, futures);
            if (System.currentTimeMillis() - startTime > processorContext.getTimeOut()) {
                throw new RuntimeException(String.format("Did not finish matching %d rows in %.2f minutes.",
                        processorContext.getBlockSize(), processorContext.getTimeOut() / 60000.0));
            }
        }

        log.info(String.format("Finished matching %d rows in %.2f minutes.", processorContext.getBlockSize(),
                (System.currentTimeMillis() - startTime) / 60000.0));
    }

    private void consumeFutures(ProcessorContext processorContext, Collection<Future<MatchContext>> futures) {
        List<Future<MatchContext>> toDelete = new ArrayList<>();

        MatchContext combinedContext = null;
        for (Future<MatchContext> future : futures) {
            MatchContext context;
            try {
                context = future.get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                continue;
            }
            // always skip this future if it has not timed out.
            toDelete.add(future);
            if (context != null) {
                if (combinedContext == null) {
                    combinedContext = context;
                } else {
                    combinedContext.getOutput().getResult().addAll(context.getOutput().getResult());
                }
                processorContext.getRowsProcessed().addAndGet(context.getInput().getNumRows());
            }
        }
        if (combinedContext != null && !combinedContext.getOutput().getResult().isEmpty()) {
            processMatchOutput(processorContext, combinedContext.getOutput());
            int rows = processorContext.getRowsProcessed().get();
            processorContext.getDataCloudProcessor().setProgress(0.07f + 0.9f * rows / processorContext.getBlockSize());
            log.info("Processed " + rows + " out of " + processorContext.getBlockSize() + " rows.");
        }

        futures.removeAll(toDelete);
    }

}
