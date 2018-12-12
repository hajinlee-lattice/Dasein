package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

/**
 * Micro batch asynchronous lookup requests and process batches in background.
 */
public abstract class DataSourceMicroBatchLookupServiceBase extends DataSourceLookupServiceBase {

    /*
     * TODO use blocking queue in batch mode (don't care about single record latency that much)
     * TODO implement graceful termination
     */

    private static final Logger log = LoggerFactory.getLogger(DataSourceMicroBatchLookupServiceBase.class);

    private volatile boolean initialized;
    private final Queue<String> pendingRequestIds = new ConcurrentLinkedQueue<>();
    private ExecutorService executorService;

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request, String returnAddress) {
        // make sure required objects are instantiated
        init();

        saveReq(lookupRequestId, returnAddress, request);
        pendingRequestIds.offer(lookupRequestId);
        synchronized (pendingRequestIds) {
            // just notify all is safer
            pendingRequestIds.notifyAll();
        }
    }

    /**
     * Return the thread pool name used for micro batching
     *
     * @return non-null pool name
     */
    protected abstract String getThreadPoolName();

    /**
     * Return micro batch size
     * @return integer > 0
     */
    protected abstract int getChunkSize();

    /**
     * Return the number of background threads that will handle batched requests
     *
     * @return integer > 0
     */
    protected abstract int getThreadCount();

    /**
     * Callback to handle batched requests. This method will be executed in one of the background threads.
     * The class that implement this method is responsible of sending response with either
     *   (a) {@link this#sendResponse(String, Object, String)} to indicate the lookup succeeded
     *   (b) {@link this#sendFailureResponse(DataSourceLookupRequest, Exception)} to indicate the lookup failed
     *
     * @param requestIds list of request IDs
     */
    protected abstract void handleRequests(List<String> requestIds);

    /*
     * helper to fail a list of requests
     */
    protected void sendFailureResponses(@NotNull List<String> requestIds, @NotNull Exception e) {
        if (CollectionUtils.isEmpty(requestIds)) {
            return;
        }

        requestIds.stream()
                .map(requestId -> Pair.of(requestId, getReq(requestId)))
                .filter(pair -> pair.getValue() != null)
                .forEach(pair -> {
                    // remove pending request and send fail response
                    removeReq(pair.getKey());
                    sendFailureResponse(pair.getRight(), e);
                });
    }

    /*
     * Worker that handle micro-batched requests in the background
     */
    private class Fetcher implements Runnable {

        @Override
        public void run() {
            int chunkSize = getChunkSize();
            String name = getClass().getSimpleName();
            while (true) {
                List<String> requestIds = new ArrayList<>();
                synchronized (pendingRequestIds) {
                    while (pendingRequestIds.isEmpty()) {
                        try {
                            pendingRequestIds.wait();
                        } catch (InterruptedException e) {
                            log.error("Encounter InterruptedException in {} fetcher, err={}", name, e.getMessage());
                        }
                    }

                    // extract current batch of requestIds
                    int size = Math.min(pendingRequestIds.size(), chunkSize);
                    for (int i = 0; i < size; i++) {
                        requestIds.add(pendingRequestIds.poll());
                    }
                }
                try {
                    handleRequests(requestIds);
                } catch (Exception e) {
                    log.error("Failed to handle requests in fetcher", e);
                }
            }
        }
    }

    /*
     * make sure executor & fetcher are instantiated properly (lazy)
     */
    private void init() {
        if (initialized) {
            return;
        }

        synchronized (this) {
            if (initialized) {
                return;
            }

            String poolName = getThreadPoolName();
            int nTreads = getThreadCount();
            log.info("Initializing fetcher for {}, nThreads = {}, ", poolName, nTreads);
            executorService = ThreadPoolUtils.getFixedSizeThreadPool(poolName, nTreads);
            IntStream.range(0, nTreads).forEach(idx -> executorService.execute(new Fetcher()));

            initialized = true;
        }
    }
}
