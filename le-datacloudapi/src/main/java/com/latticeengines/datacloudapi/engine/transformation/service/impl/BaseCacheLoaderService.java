package com.latticeengines.datacloudapi.engine.transformation.service.impl;

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
import javax.inject.Inject;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloudapi.engine.transformation.service.CacheLoaderConfig;
import com.latticeengines.datacloudapi.engine.transformation.service.CacheLoaderService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;

public abstract class BaseCacheLoaderService<E> implements CacheLoaderService<E>, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(BaseCacheLoaderService.class);

    @Value("${datacloud.match.cache.loader.batch.size:25}")
    private int batchSize;

    @Value("${datacloud.match.cache.loader.tasks.size:32}")
    private int tasksSize;

    @Value("${datacloud.match.cache.loader.thread.pool.size:16}")
    private int poolSize;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private DnBCacheService dnbCacheService;

    @Inject
    private NameLocationService nameLocationService;

    private ApplicationContext applicationContext;
    private ExecutorService executor;

    protected abstract Iterator<E> iterator(String dirPath);

    protected abstract Object getFieldValue(E record, String dunsField);

    // Map from source fields to NameLocation fields
    private static Map<String, String> defaultFieldMap = new HashMap<>();

    static {
        defaultFieldMap.put("LDC_Name", "name");
        defaultFieldMap.put("LDC_Country", "country");
        defaultFieldMap.put("LDC_State", "state");
        defaultFieldMap.put("LDC_City", "city");
        defaultFieldMap.put("LDC_ZipCode", "zipcode");
        defaultFieldMap.put("LDC_PhoneNumber", "phoneNumber");
    }
    private static String defaultDunsField = "LDC_DUNS";
    private static String defaultMatchGrade = "AAAAAAAAAAA";
    private static int defaultConfidenceCode = 10;

    @Override
    public void loadCache(CacheLoaderConfig config) {
        try {
            validateConfig(config);
            String dirPath = config.getDirPath();
            if (StringUtils.isBlank(dirPath)) {
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
        long currentRow = 0;
        while (iterator.hasNext()) {
            E record = iterator.next();
            if (config.getStartRow() != null && currentRow < config.getStartRow()) {
                currentRow++;
                continue;
            }
            if (config.getEndRow() != null && currentRow > config.getEndRow()) {
                break;
            }
            currentRow++;

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
        long recordCount = AvroUtils.count(yarnConfiguration, toAvroGlobs(dirPath));
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
            Thread.sleep(100L);
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
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
            throw new RuntimeException(String.format("Did not finish loading in %.2f minutes.",
                    72_000_000 / 60_000.0));
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

    private List<DnBMatchContext> toCacheContexts(List<E> records, CacheLoaderConfig config) {
        List<DnBMatchContext> matchContexts = new ArrayList<DnBMatchContext>();
        for (E record : records) {
            DnBMatchContext matchContext = createMatchContext(record, config);
            if (matchContext != null)
                matchContexts.add(matchContext);
        }
        return matchContexts;
    }

    private DnBMatchContext createMatchContext(E record, CacheLoaderConfig config) {
        String dunsField = getDunsField(config);
        Object duns = getFieldValue(record, dunsField);
        if ((duns == null || StringUtils.isBlank(duns.toString())) && config.isWhiteCache()) {
            return null;
        }
        if (duns != null && StringUtils.isNotBlank(duns.toString()) && !config.isWhiteCache()) {
            return null;
        }
        String dunsStr = null;
        if (config.isWhiteCache()) {
            dunsStr = duns.toString();
        }
        DnBMatchContext matchContext = new DnBMatchContext();
        createNameLocation(matchContext, record, config, dunsStr);
        return matchContext;
    }

    private String getDunsField(CacheLoaderConfig config) {
        String dunsField = config.getDunsField();
        if (StringUtils.isBlank(dunsField)) {
            dunsField = defaultDunsField;
        }
        return dunsField;
    }

    private void createNameLocation(DnBMatchContext matchContext, E record, CacheLoaderConfig config, String dunsStr) {
        NameLocation nameLocation = new NameLocation();
        setFieldValues(record, nameLocation, config);
        nameLocationService.normalize(nameLocation);
        matchContext.setInputNameLocation(nameLocation);

        matchContext.setDuns(dunsStr);
        setCondidenceCode(matchContext, record, config);
        setMatchGrade(matchContext, record, config);

        matchContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
        if (config.isWhiteCache()) {
            matchContext.setDnbCode(DnBReturnCode.OK);
        } else {
            matchContext.setDnbCode(DnBReturnCode.UNMATCH);
        }
    }

    private void setMatchGrade(DnBMatchContext matchContext, E record, CacheLoaderConfig config) {
        if (StringUtils.isNotEmpty(config.getMatchGrade())) {
            matchContext.setMatchGrade(config.getMatchGrade());
        } else {
            if (StringUtils.isNotEmpty(config.getMatchGradeField())) {
                Object matchGrade = getFieldValue(record, config.getMatchGradeField());
                if (matchGrade == null) {
                    matchContext.setMatchGrade((String)null);
                } else {
                    matchContext.setMatchGrade(matchGrade.toString());
                }
            } else {
                matchContext.setMatchGrade(defaultMatchGrade);
            }
        }
    }

    private void setCondidenceCode(DnBMatchContext matchContext, E record, CacheLoaderConfig config) {
        if (config.getConfidenceCode() != null) {
            matchContext.setConfidenceCode(config.getConfidenceCode());
        } else {
            if (StringUtils.isNotEmpty(config.getConfidenceCodeField())) {
                Object confidence = getFieldValue(record, config.getConfidenceCodeField());
                Integer confidenceCode = null;
                if (confidence != null) {
                    try {
                        confidenceCode = Integer.valueOf(confidence.toString());
                    } catch (Exception ex) {
                        log.warn("Failed to get confidence code from=" + confidence.toString());
                    }
                }
                matchContext.setConfidenceCode(confidenceCode);
            } else {
                matchContext.setConfidenceCode(defaultConfidenceCode);
            }
        }
    }

    private void setFieldValues(E record, NameLocation nameLocation, CacheLoaderConfig config) {
        Map<String, String> fieldMap = config.getFieldMap();
        if (fieldMap == null || fieldMap.size() == 0) {
            fieldMap = defaultFieldMap;
        }
        for (String key : fieldMap.keySet()) {
            Object obj = getFieldValue(record, key);
            String value = null;
            if (obj != null) {
                value = obj.toString();
            }
            try {
                BeanUtils.setProperty(nameLocation, fieldMap.get(key), value);
            } catch (Exception ex) {
                log.warn("Failed to setup field value, name location's fieldName=" + fieldMap.get(key)
                        + " record fieldName=" + key);
            }
        }
    }

    private class CacheLoaderDisplatchRunnable implements Runnable {
        private String dirPath;
        private CacheLoaderConfig config;

        CacheLoaderDisplatchRunnable(String dirPath, CacheLoaderConfig config) {
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

        CacheLoaderCallable(List<E> records, long recordStart, CacheLoaderConfig config) {
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
                    log.info("Batch has no required records, record start=" + recordStart);
                    return 0;
                }
                List<DnBCache> caches = dnbCacheService.batchAddCache(matchContexts);
                log.info("Finished loading cache! record start=" + recordStart + " batch size=" + records.size()
                        + " cache size=" + matchContexts.size() + " returned size=" + caches.size());
                return caches.size();
            } catch (Exception ex) {
                log.info("Failed to load cache! record start=" + recordStart + " batch size=" + records.size(), ex);
                return 0;
            }
        }
    }

    protected static String toAvroGlobs(String avroDirOrFile) {
        String avroGlobs = avroDirOrFile;
        if (!avroDirOrFile.endsWith(".avro")) {
            avroGlobs += "/*.avro";
        }
        return avroGlobs;
    }
}
