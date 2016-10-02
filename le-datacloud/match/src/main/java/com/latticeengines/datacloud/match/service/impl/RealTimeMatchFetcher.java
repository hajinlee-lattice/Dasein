package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.datacloud.match.service.MatchFetcher;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

@Component("realTimeMatchFetcher")
public class RealTimeMatchFetcher implements MatchFetcher {

    private static final Log log = LogFactory.getLog(RealTimeMatchFetcher.class);
    private static final Integer QUEUE_SIZE = 20000;
    private static final Integer TIMEOUT_MINUTE = 10;

    private final BlockingQueue<MatchContext> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final ConcurrentMap<String, MatchContext> map = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> fetcherActivity = new ConcurrentHashMap<>();
    private ExecutorService executor;

    @Value("${datacloud.match.realtime.group.size:20}")
    private Integer groupSize;

    @Value("${datacloud.match.num.fetchers:16}")
    private Integer numFetchers;

    @Value("${datacloud.match.realtime.fetchers.enable:false}")
    private boolean enableFetchers;

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    private boolean fetchersInitiated = false;

    @PostConstruct
    private void postConstruct() {
        if (enableFetchers) {
            initExecutors();
        }
    }

    public void initExecutors() {
        if (fetchersInitiated) {
            // do nothing if fetcher executors are already started
            return;
        }

        log.info("Initialize propdata fetcher executors.");
        executor = Executors.newFixedThreadPool(numFetchers);
        for (int i = 0; i < numFetchers; i++) {
            executor.submit(new Fetcher());
        }
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                scanQueue();
            }
        }, TimeUnit.SECONDS.toMillis(10));

        fetchersInitiated = true;
    }

    @Override
    @MatchStep
    public MatchContext fetch(MatchContext matchContext) {
        if (enableFetchers) {
            queue.add(matchContext);
            return waitForResult(matchContext.getOutput().getRootOperationUID());
        } else {
            return fetchSync(matchContext);
        }
    }

    private MatchContext fetchSync(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        return dbHelper.fetch(matchContext);
    }

    private void scanQueue() {
        int numNewFetchers = 0;
        synchronized (fetcherActivity) {
            for (Map.Entry<String, Long> entry : fetcherActivity.entrySet()) {
                if (System.currentTimeMillis() - entry.getValue() > TimeUnit.HOURS.toMillis(1)) {
                    log.warn("Fetcher " + entry.getKey() + " has no activity for 1 hour. Spawn a new one");
                    fetcherActivity.remove(entry.getKey());
                }
            }
            numNewFetchers = numNewFetchers - fetcherActivity.size();
        }
        for (int i = 0; i < numNewFetchers; i++) {
            executor.submit(new Fetcher());
        }
    }

    private MatchContext waitForResult(String rootUid) {
        log.debug("Waiting for result of RootOperationUID=" + rootUid);
        Long startTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(100L);
            } catch (Exception e) {
                log.error("Interrupted when waiting for fetch result. RootOperationUID=" + rootUid, e);
            }
            if (map.containsKey(rootUid)) {
                log.debug("Found fetch result for RootOperationUID=" + rootUid);
                return map.remove(rootUid);
            }
        } while (System.currentTimeMillis() - startTime < TimeUnit.MINUTES.toMillis(TIMEOUT_MINUTE));
        throw new RuntimeException("Fetching timeout. RootOperationUID=" + rootUid);
    }

    private class Fetcher implements Runnable {
        private String groupDataCloudVersion;

        @Override
        public void run() {
            String name = Thread.currentThread().getName();
            log.info("Launched a fetcher " + name);
            while (true) {
                try {
                    fetcherActivity.put(name, System.currentTimeMillis());
                    groupDataCloudVersion = null;

                    while (!queue.isEmpty()) {
                        List<MatchContext> matchContextList = new ArrayList<>();
                        int thisGroupSize = Math.min(groupSize, Math.max(queue.size() / 4, 4));
                        int inGroup = 0;
                        while (inGroup < thisGroupSize && !queue.isEmpty()) {
                            try {
                                MatchContext matchContext = queue.poll(50, TimeUnit.MILLISECONDS);
                                if (matchContext != null) {
                                    String version = matchContext.getInput().getDataCloudVersion();
                                    if (StringUtils.isEmpty(version)) {
                                        version = MatchInput.DEFAULT_DATACLOUD_VERSION;
                                    }
                                    if (StringUtils.isEmpty(groupDataCloudVersion)) {
                                        groupDataCloudVersion = version;
                                    }
                                    if (version.equals(groupDataCloudVersion)) {
                                        matchContextList.add(matchContext);
                                        inGroup += matchContext.getInput().getData().size();
                                    } else {
                                        log.info("Found a match context with a version, "
                                                + matchContext.getInput().getDataCloudVersion()
                                                + " different from that in current group, " + groupDataCloudVersion
                                                + ". Putting it back to the queue");
                                        queue.add(matchContext);
                                        break;
                                    }
                                }
                            } catch (InterruptedException e) {
                                // skip
                            }
                        }

                        if (matchContextList.size() == 1) {
                            MatchContext matchContext = fetchSync(matchContextList.get(0));
                            map.putIfAbsent(matchContext.getOutput().getRootOperationUID(), matchContext);
                            log.debug("Put the result for " + matchContext.getOutput().getRootOperationUID()
                                    + " back into concurrent map.");
                        } else {
                            fetchMultipleContexts(matchContextList);
                        }
                        fetcherActivity.put(name, System.currentTimeMillis());
                    }
                } catch (Exception e) {
                    log.warn("Error from fetcher.", e);
                } finally {
                    try {
                        Thread.sleep(50L);
                    } catch (Exception e1) {
                        // ignore
                    }
                }
            }
        }

        private void fetchMultipleContexts(List<MatchContext> matchContextList) {
            try {
                if (!matchContextList.isEmpty()) {
                    DbHelper dbHelper = beanDispatcher.getDbHelper(groupDataCloudVersion);
                    MatchContext mergedContext = dbHelper.mergeContexts(matchContextList, groupDataCloudVersion);
                    mergedContext = dbHelper.fetch(mergedContext);
                    dbHelper.splitContext(mergedContext, matchContextList);
                    for (MatchContext context : matchContextList) {
                        String rootUid = context.getOutput().getRootOperationUID();
                        map.putIfAbsent(rootUid, context);
                        log.debug("Put match context to concurrent map for RootOperationUID=" + rootUid);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to fetch multi-context match input.", e);
            }
        }

    }

    @Override
    public List<String> enqueue(List<MatchContext> matchContexts) {
        List<String> rootUids = new ArrayList<>(matchContexts.size());

        if (enableFetchers) {
            log.info("Enqueueing " + matchContexts.size() + " match contexts");
            queue.addAll(matchContexts);
            log.info("Enqueued " + matchContexts.size() + " match contexts");
            for (MatchContext context : matchContexts) {
                rootUids.add(context.getOutput().getRootOperationUID());
            }
        } else {
            for (MatchContext context : matchContexts) {
                String uuid = context.getOutput().getRootOperationUID();
                map.putIfAbsent(uuid, fetchSync(context));
                rootUids.add(uuid);
            }
        }

        return rootUids;
    }

    @Override
    public List<MatchContext> waitForResult(List<String> rootUids) {
        Map<String, MatchContext> intermediateResults = new HashMap<>();

        int foundResultCount = 0;
        log.debug("Waiting for results of RootOperationUIDs=" + rootUids);
        Long startTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(100L);
            } catch (Exception e) {
                log.error("Interrupted when waiting for fetch result. RootOperationUID=" + rootUids, e);
            }

            for (String rootUid : rootUids) {
                MatchContext storedResult = intermediateResults.get(rootUid);
                if (storedResult == null && map.containsKey(rootUid)) {
                    log.debug(foundResultCount + ": Found fetch result for RootOperationUID=" + rootUid);
                    intermediateResults.put(rootUid, map.remove(rootUid));
                    foundResultCount++;

                    if (foundResultCount >= rootUids.size()) {
                        return convertResultMapToList(rootUids, intermediateResults);
                    }
                }

            }
        } while (System.currentTimeMillis() - startTime < TimeUnit.MINUTES.toMillis(TIMEOUT_MINUTE));
        throw new RuntimeException("Fetching timeout. RootOperationUID=" + rootUids);
    }

    private List<MatchContext> convertResultMapToList(List<String> rootUids,
            Map<String, MatchContext> intermediateResults) {
        List<MatchContext> results = new ArrayList<>(rootUids.size());
        for (String rootUid : rootUids) {
            results.add(intermediateResults.get(rootUid));
        }
        return results;
    }
}
