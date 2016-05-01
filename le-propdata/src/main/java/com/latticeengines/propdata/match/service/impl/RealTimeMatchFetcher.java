package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.monitor.exposed.metric.service.StatsService;
import com.latticeengines.monitor.exposed.metric.stats.impl.CollectionSizeInspection;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchFetcher;

@Component("realTimeMatchFetcher")
public class RealTimeMatchFetcher extends MatchFetcherBase implements MatchFetcher {

    private static final Log log = LogFactory.getLog(RealTimeMatchFetcher.class);
    private static final Integer QUEUE_SIZE = 1000;
    private static final Integer TIMEOUT_HOURS = 1;
    private static AtomicBoolean inspectionRegistered = new AtomicBoolean(false);

    private final BlockingQueue<MatchContext> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final ConcurrentMap<String, MatchContext> map = new ConcurrentHashMap<>();
    private ExecutorService executor;

    @Value("${propdata.match.group.size:20}")
    private Integer groupSize;

    @Value("${propdata.match.num.fetchers:16}")
    private Integer numFetchers;

    @Autowired
    @Qualifier("monitorScheduler")
    private ThreadPoolTaskScheduler monitorScheduler;

    @Autowired
    private StatsService statsService;

    @PostConstruct
    private void postConstruct() {
        executor = Executors.newFixedThreadPool(numFetchers);
        for (int i = 0; i < numFetchers; i++) {
            executor.submit(new Fetcher());
        }
    }

    @Override
    @MatchStep
    public MatchContext fetch(MatchContext matchContext) {
        if (!inspectionRegistered.get()) {
            statsService.register(new CollectionSizeInspection(monitorScheduler, queue, "propdata-fetch-queue"));
            inspectionRegistered.set(true);
        }

        queue.add(matchContext);
        return waitForResult(matchContext.getOutput().getRootOperationUID());
    }

    private MatchContext waitForResult(String rootUid) {
        log.debug("Waiting for result of RootOperationUID=" + rootUid);
        Long startTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(200L);
            } catch (Exception e) {
                log.error("Interrupted when waiting for fetch result. RootOperationUID=" + rootUid, e);
            }
            if (map.containsKey(rootUid)) {
                log.debug("Found fetch result for RootOperationUID=" + rootUid);
                return map.remove(rootUid);
            }
        } while (System.currentTimeMillis() - startTime < TIMEOUT_HOURS * 3600 * 1000L);
        throw new RuntimeException("Fetching timeout. RootOperationUID=" + rootUid);
    }

    private class Fetcher implements Runnable {
        @Override
        public void run() {
            log.info("Launched a fetcher.");
            while (true) {
                try {
                    while (!queue.isEmpty()) {
                        List<MatchContext> matchContextList = new ArrayList<>();
                        synchronized (queue) {
                            int thisGroupSize = Math.min(groupSize, Math.max(queue.size() / numFetchers, 1));
                            int inGroup = 0;
                            while (inGroup < thisGroupSize && !queue.isEmpty()) {
                                matchContextList.add(queue.poll());
                            }
                        }

                        if (matchContextList.size() == 1) {
                            MatchContext matchContext = fetchSync(matchContextList.get(0));
                            map.putIfAbsent(matchContext.getOutput().getRootOperationUID(), matchContext);
                        } else {
                            fetchMultipleContexts(matchContextList);
                        }
                        try {
                            Thread.sleep(50L);
                        } catch (Exception e1) {
                            // ignore
                        }
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
                    MatchContext mergedContext = mergeContexts(matchContextList);
                    mergedContext = fetchSync(mergedContext);
                    splitContext(mergedContext, matchContextList);
                }
            } catch (Exception e) {
                log.error(e);
            }
        }

        private MatchContext mergeContexts(List<MatchContext> matchContextList) {
            MatchContext mergedContext = new MatchContext();
            Set<String> domainSet = new HashSet<>();
            Set<NameLocation> nameLocationSet = new HashSet<>();
            Map<String, Set<String>> srcColSetMap = new HashMap<>();
            for (MatchContext matchContext : matchContextList) {
                domainSet.addAll(matchContext.getDomains());
                nameLocationSet.addAll(matchContext.getNameLocations());
                Map<String, List<String>> srcColMap1 = matchContext.getSourceColumnsMap();
                for (Map.Entry<String, List<String>> entry : srcColMap1.entrySet()) {
                    String sourceName = entry.getKey();
                    if (srcColSetMap.containsKey(sourceName)) {
                        srcColSetMap.get(sourceName).addAll(entry.getValue());
                    } else {
                        srcColSetMap.put(sourceName, new HashSet<>(entry.getValue()));
                    }
                }
            }

            Map<String, List<String>> srcColMap = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : srcColSetMap.entrySet()) {
                srcColMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            mergedContext.setDomains(domainSet);
            mergedContext.setNameLocations(nameLocationSet);
            mergedContext.setSourceColumnsMap(srcColMap);
            return mergedContext;
        }

        private void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList) {
            Map<String, List<Map<String, Object>>> allResults = mergedContext.getResultsBySource();

            for (MatchContext context : matchContextList) {
                Map<String, List<String>> srcColMap = context.getSourceColumnsMap();
                Map<String, List<Map<String, Object>>> result = new HashMap<>();
                for (Map.Entry<String, List<String>> entry : srcColMap.entrySet()) {
                    String sourceName = entry.getKey();
                    if (!allResults.containsKey(sourceName)) {
                        throw new RuntimeException("Merged result does not have required source " + sourceName);
                    }
                    List<Map<String, Object>> mergedResult = allResults.get(sourceName);
                    result.put(sourceName, new ArrayList<>(mergedResult));
                }
                context.setResultsBySource(result);
                String rootUid = context.getOutput().getRootOperationUID();
                map.putIfAbsent(rootUid, context);
                log.debug("Put match context to concurrent map for RootOperationUID=" + rootUid);
            }
        }

    }

}
