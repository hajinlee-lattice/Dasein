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

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

@Component("bulkMatchProcessorExecutor")
public class BulkMatchProcessorExecutorImpl extends AbstractBulkMatchProcessorExecutorImpl {

    private static final Logger log = LoggerFactory.getLogger(BulkMatchProcessorExecutorImpl.class);

    @Override
    public void execute(ProcessorContext processorContext) {

        ExecutorService executor = ThreadPoolUtils.getFixedSizeThreadPool("bulk-match",
                processorContext.getNumThreads());
        processorContext.getDataCloudProcessor().setProgress(0.07f);
        Long startTime = System.currentTimeMillis();
        Set<Future<MatchContext>> futures = new HashSet<>();
        while (processorContext.getDivider().hasNextGroup()) {
            if (futures.size() < processorContext.getNumThreads()) {
                // create new input object for each record
                MatchInput input = constructMatchInputFromData(processorContext);
                // cache an input to generate output metric
                if (processorContext.getGroupMatchInput() == null) {
                    MatchInput matchInput = JsonUtils.deserialize(JsonUtils.serialize(input), MatchInput.class);
                    processorContext.setGroupMatchInput(matchInput);
                }
                Future<MatchContext> future;
                if (processorContext.isUseProxy()) {
                    future = executor.submit(new RealTimeMatchCallable(input, processorContext.getPodId(), matchProxy));
                } else {
                    future = executor.submit(
                            new BulkMatchCallable(input, processorContext.getPodId(), matchPlanner, matchExecutor));
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
            } catch (TimeoutException | InterruptedException e) {
                continue;
            } catch (ExecutionException e) {
                throw new RuntimeException("ExecutionException in the thread, "
                        + "not sure if match result will be impacted. Terminate the block", e);
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
        if (combinedContext != null) {
            processMatchOutput(processorContext, combinedContext.getOutput());
            int rows = processorContext.getRowsProcessed().get();
            processorContext.getDataCloudProcessor().setProgress(0.07f + 0.9f * rows / processorContext.getBlockSize());
            log.info("Processed " + rows + " out of " + processorContext.getBlockSize() + " rows.");
        }

        futures.removeAll(toDelete);
    }

}
