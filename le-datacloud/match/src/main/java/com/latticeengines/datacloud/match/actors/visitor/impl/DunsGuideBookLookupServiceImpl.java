package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TERMINATE_EXECUTOR_TIMEOUT_MS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.DunsGuideBookLookupService;
import com.latticeengines.datacloud.match.exposed.service.DunsGuideBookService;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

/**
 * Service to batch {@link DunsGuideBook} retrieval request
 */
@Component("dunsGuideBookLookupService")
public class DunsGuideBookLookupServiceImpl extends DataSourceLookupServiceBase implements DunsGuideBookLookupService {
    private static final String THREAD_POOL_NAME = "dunsguidebook-fetcher";

    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookLookupServiceImpl.class);

    @Value("${datacloud.match.dynamo.fetchers.num}")
    private Integer nFetcher;

    @Value("${datacloud.match.num.dynamo.fetchers.batch.num}")
    private Integer nBatchFetcher;

    @Value("${datacloud.match.dynamo.fetchers.chunk.size}")
    private Integer chunkSize;

    @Autowired
    private DunsGuideBookService dunsGuideBookService;

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final Queue<String> pendingRequestIds = new ConcurrentLinkedQueue<>();

    private ExecutorService executorService;

    // flag to indicate whether background fetcher should keep running
    private volatile boolean shouldTerminate = false;

    @Override
    protected DunsGuideBook lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple tuple = (MatchKeyTuple) request.getInputData();
        String duns = tuple.getDuns();
        String dataCloudVersion = request.getMatchTravelerContext().getDataCloudVersion();

        long startTimestamp = System.currentTimeMillis();
        DunsGuideBook result = dunsGuideBookService.get(dataCloudVersion, duns);
        log.info("Fetched DunsGuideBook for 1 sync request, DUNS={}, DataCloudVersion={}, Duration={} ms, Found Result={}",
                duns, dataCloudVersion, System.currentTimeMillis() - startTimestamp, result != null);
        return result;
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request, String returnAddress) {
        // lazy load executor
        if (!initialized.get()) {
            initExecutor();
        }

        saveReq(lookupRequestId, returnAddress, request);
        pendingRequestIds.offer(lookupRequestId);
        synchronized (pendingRequestIds) {
            if (!pendingRequestIds.isEmpty() && pendingRequestIds.size() <= chunkSize) {
                pendingRequestIds.notify();
            } else if (pendingRequestIds.size() > chunkSize) {
                pendingRequestIds.notifyAll();
            }
        }
    }

    @Override
    public Map<String, Integer> getPendingReqStats() {
        return Collections.singletonMap(MatchConstants.REQUEST_NUM, pendingRequestIds.size());
    }

    private void initExecutor() {
        synchronized (initialized) {
            if (initialized.get()) {
                // already initialized
                return;
            }

            log.info("Initializing DunsGuideBook fetcher executor");
            Integer nTreads = isBatchMode() ? nBatchFetcher : nFetcher;
            executorService = ThreadPoolUtils.getFixedSizeThreadPool(THREAD_POOL_NAME, nTreads);

            for (int i = 0; i < nTreads; i++) {
                executorService.submit(new Fetcher());
            }

            initialized.set(true);
        }
    }

    @PreDestroy
    private void preDestroy() {
        try {
            if (shouldTerminate) {
                return;
            }
            log.info("Shutting down DunsGuideBook fetchers");
            shouldTerminate = true;
            if (executorService != null) {
                executorService.shutdownNow();
                executorService.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            log.info("Completed shutting down of DunsGuideBook fetchers");
        } catch (Exception e) {
            log.error("Fail to finish all pre-destroy actions", e);
        }
    }

    /*
     * background worker to fetch DunsGuideBook
     */
    private class Fetcher implements Runnable {
        @Override
        public void run() {
            while (!shouldTerminate) {
                List<String> requestIds = new ArrayList<>();
                synchronized (pendingRequestIds) {
                    while (!shouldTerminate && pendingRequestIds.isEmpty()) {
                        try {
                            pendingRequestIds.wait();
                        } catch (InterruptedException e) {
                            if (!shouldTerminate) {
                                log.warn("Encounter InterruptedException in DunsGuideBook fetcher, err={}",
                                        e.getMessage());
                            }
                        }
                    }

                    // extract current batch of requestIds
                    int size = Math.min(pendingRequestIds.size(), chunkSize);
                    for (int i = 0; i < size; i++) {
                        requestIds.add(pendingRequestIds.poll());
                    }
                }
                handleRequests(requestIds);
            }
        }

        /*
         * handle the current batch of requests
         */
        private void handleRequests(List<String> requestIds) {
            // key = dataCloudVersion, value = list of pair<requestId,source duns>
            Map<String, List<Pair<String, String>>> paramMap = new HashMap<>();
            for (String id : requestIds) {
                DataSourceLookupRequest req = getReq(id);
                String dataCloudVersion = req.getMatchTravelerContext().getDataCloudVersion();
                MatchKeyTuple tuple = (MatchKeyTuple) req.getInputData();
                String duns = tuple.getDuns();
                paramMap.putIfAbsent(dataCloudVersion, new ArrayList<>());
                paramMap.get(dataCloudVersion).add(Pair.of(id, duns));
            }

            for (String dataCloudVersion : paramMap.keySet()) {
                handleRequests(dataCloudVersion, paramMap.get(dataCloudVersion));
            }
        }

        /*
         * handle requests in the same dataCloudVersion
         */
        private void handleRequests(String dataCloudVersion, List<Pair<String, String>> params) {
            try {
                List<String> dunsList = params.stream().map(Pair::getValue).collect(Collectors.toList());
                List<DunsGuideBook> books = dunsGuideBookService.get(dataCloudVersion, dunsList);
                Preconditions.checkNotNull(books);
                Preconditions.checkArgument(books.size() == params.size());

                // send responses
                for (int i = 0; i < params.size(); i++) {
                    String requestId = params.get(i).getKey();
                    String returnAddr = getReqReturnAddr(requestId);
                    // Inject failure only for testing purpose
                    injectFailure(getReq(requestId));
                    removeReq(requestId);
                    sendResponse(requestId, books.get(i), returnAddr);
                }
            } catch (Exception e) {
                String msg = String.format("Failed to retrieve batch DunsGuideBooks, size=%d", params.size());
                log.error(msg, e);
                // consider all request failed
                params.forEach(param -> {
                    String requestId = param.getKey();
                    DataSourceLookupRequest req = getReq(requestId);
                    if (req != null) {
                        removeReq(requestId);
                        sendFailureResponse(req, e);
                    }
                });
            }
        }
    }
}
