package com.latticeengines.matchapi.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.matchapi.service.CacheLoaderConfig;
import com.latticeengines.matchapi.service.CacheLoaderService;

public abstract class BaseCacheLoaderService<E> implements CacheLoaderService<E>, ApplicationContextAware {

    @Value("${datacloud.match.cache.loader.batch.size:25}")
    private int batchSize;

    @Value("${datacloud.match.cache.loader.tasks.size:32}")
    private int tasksSize;

    @Value("${datacloud.match.cache.loader.thread.pool.size:8}")
    private int poolSize;

    private static Log log = LogFactory.getLog(BaseCacheLoaderService.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;
    @Autowired
    protected Configuration yarnConfiguration;
    @Autowired
    private DnBCacheService dnbCacheService;

    private ApplicationContext applicationContext;
    private ExecutorService executor;

    protected abstract Iterator<E> iterator(String dirPath);

    protected abstract DnBMatchContext createMatchContext(E record, CacheLoaderConfig config);

    // Map from source fields to NameLocation fields
    protected static Map<String, String> defaultFieldMap = new HashMap<>();
    static {
        defaultFieldMap.put("LDC_Name", "name");
        defaultFieldMap.put("LDC_Country", "countryCode");
        defaultFieldMap.put("LDC_State", "state");
        defaultFieldMap.put("LDC_City", "city");
        defaultFieldMap.put("LDC_PhoneNumber", "phoneNumber");
        defaultFieldMap.put("LDC_ZipCode", "zipcode");
    }

    protected static String defaultDunsField = "LDC_DUNS";

    @Override
    public void loadCache(CacheLoaderConfig config) {
        try {
            validateConfig(config);
            String dirPath = config.getDirPath();
            if (StringUtils.isEmpty(dirPath)) {
                dirPath = getDirPathWithSource(config);
            }
            CacheLoaderDisplatchRunnable runnable = new CacheLoaderDisplatchRunnable(dirPath, config);
            Thread thread = new Thread(runnable);
            thread.start();

        } catch (Exception ex) {
            log.error("Failed to load cache!", ex);
            throw new RuntimeException(ex);
        }
    }

    protected long startLoad(String dirPath, CacheLoaderConfig config) throws Exception {
        log.info("Started to load cache on path=" + dirPath);
        long startTime = System.currentTimeMillis();
        Iterator<E> iterator = iterator(dirPath);
        List<E> records = new ArrayList<>();
        List<Future<Integer>> futures = new ArrayList<>();
        AtomicLong counter = new AtomicLong();
        long recordStart = 0;
        while (iterator.hasNext()) {
            E record = iterator.next();
            records.add(record);
            if (records.size() >= batchSize) {
                Future<Integer> future = executor
                        .submit(new CacheLoaderCallable<Boolean>(records, recordStart, config));
                recordStart += records.size();
                records = new ArrayList<>();
                futures.add(future);
                if (futures.size() > tasksSize) {
                    processFutures(futures, poolSize, counter, startTime);
                }
            }
        }
        if (records.size() > 0) {
            Future<Integer> future = executor.submit(new CacheLoaderCallable<Boolean>(records, recordStart, config));
            futures.add(future);
        }
        log.info("Last Batch! record start=" + recordStart);
        processFutures(futures, 0, counter, startTime);

        resportResult(dirPath, startTime, counter);
        return counter.longValue();
    }

    private void resportResult(String dirPath, long startTime, AtomicLong counter) {
        long recordCount = AvroUtils.count(yarnConfiguration, MatchUtils.toAvroGlobs(dirPath));
        long endTime = System.currentTimeMillis();
        log.info("Finished loading all cache on path=" + dirPath + " total records=" + recordCount
                + " total cached size=" + counter + " Time spent="
                + DurationFormatUtils.formatDuration(endTime - startTime, "HH:mm:ss:SS"));
    }

    private void processFutures(List<Future<Integer>> futures, int futureThreshold, AtomicLong counter, long startTime) {
        while (futures.size() > futureThreshold) {
            List<Future<Integer>> doneFutures = new ArrayList<>();
            for (Future<Integer> future : futures) {
                consumeFutures(counter, doneFutures, future);
            }
            if (doneFutures.size() > 0) {
                futures.removeAll(doneFutures);
            }
            if (futures.size() > futureThreshold) {
                sleepForSeconds();
            }
            checkTimeout(startTime);
        }
    }

    private void sleepForSeconds() {
        try {
            log.info("Many records, sleep for seconds");
            Thread.sleep(5_000L);
        } catch (Exception ex) {
            log.warn(ex);
        }
    }

    private void consumeFutures(AtomicLong counter, List<Future<Integer>> doneFutures, Future<Integer> future) {
        if (future.isDone()) {
            try {
                Integer batchCount = future.get(5, TimeUnit.MINUTES);
                counter.addAndGet(batchCount.longValue());
                doneFutures.add(future);
            } catch (Exception ex) {
                log.warn("Failed to get batch count! msg=" + ex.getMessage());
            }
        }
    }

    private void checkTimeout(Long startTime) {
        if (System.currentTimeMillis() - startTime > 72_000_000) {
            throw new RuntimeException(String.format("Did not finish matching %d rows in %.2f minutes.",
                    18_000_000 / 60_000.0));
        }
    }

    private void validateConfig(CacheLoaderConfig config) {
        log.info("Cache loader's config=" + config);
        if (config.getSourceName() == null && config.getDirPath() == null) {
            throw new RuntimeException("No source name or file path specified!");
        }
    }

    private String getDirPathWithSource(CacheLoaderConfig config) {
        return hdfsPathBuilder.constructTransformationSourceDir(getSource(config), config.getVersion()).toString();
    }

    private Source getSource(CacheLoaderConfig config) {
        return (Source) applicationContext.getBean(config.getSourceName());
    }

    @PostConstruct
    public void init() {
        executor = Executors.newFixedThreadPool(poolSize);
    }

    @PreDestroy
    public void finish() {
        try {
            executor.shutdownNow();
        } catch (Exception ex) {
            log.warn("Failed on finish! msg=" + ex.getMessage());
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private List<DnBMatchContext> toCacheContexts(List<E> matchRecords, CacheLoaderConfig config) {
        List<DnBMatchContext> matchContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < matchRecords.size(); i++) {
            DnBMatchContext matchContext = createMatchContext(matchRecords.get(i), config);
            if (matchContext != null)
                matchContexts.add(matchContext);
        }
        return matchContexts;
    }

    private class CacheLoaderDisplatchRunnable implements Runnable {
        private String dirPath;
        private CacheLoaderConfig config;

        public CacheLoaderDisplatchRunnable(String dirPath, CacheLoaderConfig config) {
            this.dirPath = dirPath;
            this.config = config;
        }

        @Override
        public void run() {
            try {
                startLoad(dirPath, config);
            } catch (Exception ex) {
                log.info("Failed to load cache!", ex);
            }
        }
    }

    private class CacheLoaderCallable<T> implements Callable<Integer> {
        private List<E> records;
        private long recordStart;
        private CacheLoaderConfig config;

        public CacheLoaderCallable(List<E> records, long recordStart, CacheLoaderConfig config) {
            this.records = records;
            this.recordStart = recordStart;
            this.config = config;
        }

        @Override
        public Integer call() {
            try {
                log.info("Starting to load cache! record start=" + recordStart + " batch size=" + records.size());
                List<DnBMatchContext> matchContexts = toCacheContexts(records, config);
                if (matchContexts.size() == 0) {
                    log.info("Batch has no DUNS#, record start=" + recordStart);
                    return 0;
                }
                List<DnBWhiteCache> caches = dnbCacheService.batchAddWhiteCache(matchContexts);
                log.info("Finished loading cache! record start=" + recordStart + " batch size=" + records.size()
                        + " cache size=" + matchContexts.size() + " returned size=" + caches.size());
                return caches.size();
            } catch (Exception ex) {
                log.info("Failed to load cache! record start=" + recordStart + " batch size=" + records.size(), ex);
                return 0;
            }
        }
    }
}
