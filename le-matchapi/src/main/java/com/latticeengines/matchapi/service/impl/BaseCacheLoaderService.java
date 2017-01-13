package com.latticeengines.matchapi.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.DnBCacheService;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.NameLocationService;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.service.CacheLoaderConfig;
import com.latticeengines.matchapi.service.CacheLoaderService;

public abstract class BaseCacheLoaderService<E> implements CacheLoaderService<E>, ApplicationContextAware {

    @Value("${datacloud.match.cache.loader.batch.size:25}")
    private int batchSize;

    @Value("${datacloud.match.cache.loader.tasks.size:32}")
    private int tasksSize;

    @Value("${datacloud.match.cache.loader.thread.pool.size:8}")
    private int poolSize;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    private static Log log = LogFactory.getLog(BaseCacheLoaderService.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;
    @Autowired
    protected Configuration yarnConfiguration;
    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    @Qualifier("bulkMatchPlanner")
    protected MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("bulkMatchExecutor")
    protected MatchExecutor matchExecutor;

    @Autowired
    private NameLocationService nameLocationService;

    @Autowired
    private MatchActorSystem matchActorSystem;

    private ApplicationContext applicationContext;
    private ExecutorService executor;

    protected abstract Iterator<E> iterator(String dirPath);

    protected abstract Object getFieldValue(E record, String dunsField);

    // Map from source fields to NameLocation fields
    protected static Map<String, String> defaultFieldMap = new HashMap<>();
    static {
        defaultFieldMap.put("LDC_Name", "name");
        defaultFieldMap.put("LDC_Country", "country");
        defaultFieldMap.put("LDC_State", "state");
        defaultFieldMap.put("LDC_City", "city");
        defaultFieldMap.put("LDC_ZipCode", "zipcode");
        defaultFieldMap.put("LDC_PhoneNumber", "phoneNumber");
    }
    protected static String defaultDunsField = "LDC_DUNS";
    protected static String defaultMatchGrade = "AAAAAAAAA";
    protected static int defaultConfidenceCode = 10;

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
        } finally {
            matchActorSystem.setBatchMode(false);
        }
    }

    protected long startLoad(String dirPath, CacheLoaderConfig config) throws Exception {
        matchActorSystem.setBatchMode(config.isBatchMode());
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

    private List<DnBMatchContext> toCacheContexts(List<E> records, CacheLoaderConfig config) {
        List<DnBMatchContext> matchContexts = new ArrayList<DnBMatchContext>();
        for (int i = 0; i < records.size(); i++) {
            DnBMatchContext matchContext = createMatchContext(records.get(i), config);
            if (matchContext != null)
                matchContexts.add(matchContext);
        }
        return matchContexts;
    }

    private DnBMatchContext createMatchContext(E record, CacheLoaderConfig config) {
        String dunsField = getDunsField(config);
        Object duns = getFieldValue(record, dunsField);
        if (duns == null && !config.isCallMatch()) {
            return null;
        }
        if (duns != null && config.isCallMatch()) {
            return null;
        }
        String dunsStr = null;
        if (!config.isCallMatch()) {
            dunsStr = duns.toString();
        }
        if (StringUtils.isEmpty(dunsStr) && !config.isCallMatch()) {
            return null;
        }
        if (!StringUtils.isEmpty(dunsStr) && config.isCallMatch()) {
            return null;
        }
        DnBMatchContext matchContext = new DnBMatchContext();
        createNameLocation(matchContext, record, config, dunsStr);
        return matchContext;
    }

    private String getDunsField(CacheLoaderConfig config) {
        String dunsField = config.getDunsField();
        if (StringUtils.isEmpty(dunsField)) {
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
        if (config.getConfidenceCode() != null) {
            matchContext.setConfidenceCode(config.getConfidenceCode());
        } else {
            matchContext.setConfidenceCode(defaultConfidenceCode);
        }
        if (StringUtils.isNotEmpty(config.getMatchGrade())) {
            matchContext.setMatchGrade(config.getMatchGrade());
        } else {
            matchContext.setMatchGrade(defaultMatchGrade);
        }
        matchContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
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
                    log.info("Batch has no required records, record start=" + recordStart);
                    return 0;
                }
                if (config.isCallMatch()) {
                    return callMatchSync(config, matchContexts, records, recordStart);
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

        public int callMatchSync(CacheLoaderConfig config, List<DnBMatchContext> matchContexts, List<E> records,
                long recordStart) {

            MatchInput input = getMatchInput(config, matchContexts);
            MatchContext matchContext = matchPlanner.plan(input);
            matchContext = matchExecutor.execute(matchContext);
            List<OutputRecord> results = matchContext.getOutput().getResult();
            int matchSize = matchContext.getOutput().getStatistics().getRowsMatched();
            log.info("Finished matching and loading cache! record start=" + recordStart + " batch size="
                    + records.size() + " input size=" + matchContexts.size() + " returned size=" + results.size()
                    + " matched size=" + matchSize);
            return results.size();
        }

        private MatchInput getMatchInput(CacheLoaderConfig config, List<DnBMatchContext> matchContexts) {
            MatchInput matchInput = new MatchInput();
            matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
            matchInput.setTenant(new Tenant("CacheLoader"));
            matchInput.setPredefinedSelection(Predefined.ID);
            matchInput.setPredefinedVersion("1.0");
            matchInput.setMatchEngine(MatchContext.MatchEngine.BULK.getName());
            matchInput.setFields(getFields());
            matchInput.setKeyMap(getKeyMap());
            matchInput.setData(toMatchRecords(config, matchContexts));
            matchInput.setDecisionGraph("DragonClaw");
            matchInput.setExcludeUnmatchedWithPublicDomain(false);
            matchInput.setPublicDomainAsNormalDomain(true);
            matchInput.setDataCloudVersion(getDataCloudVersion(config));
            matchInput.setSkipKeyResolution(true);
            matchInput.setUseDnBCache(false);
            matchInput.setUseRemoteDnB(true);
            matchInput.setLogDnBBulkResult(false);

            return matchInput;
        }

        private List<List<Object>> toMatchRecords(CacheLoaderConfig config, List<DnBMatchContext> matchContexts) {
            List<List<Object>> data = new ArrayList<List<Object>>();
            for (int i = 0; i < matchContexts.size(); i++) {
                DnBMatchContext matchContext = matchContexts.get(i);
                NameLocation nameLocation = matchContext.getInputNameLocation();
                List<Object> datum = new ArrayList<>();
                datum.add(nameLocation.getName());
                datum.add(nameLocation.getCountryCode());
                datum.add(nameLocation.getState());
                datum.add(nameLocation.getCity());
                datum.add(nameLocation.getZipcode());
                datum.add(nameLocation.getPhoneNumber());
                data.add(datum);
            }
            return data;
        }

        private String getDataCloudVersion(CacheLoaderConfig config) {
            String dataCloudVersion = config.getDataCloudVersion();
            if (StringUtils.isEmpty(dataCloudVersion)) {
                dataCloudVersion = versionEntityMgr.currentApprovedVersion().getVersion();
            }
            return dataCloudVersion;
        }

        private Map<MatchKey, List<String>> getKeyMap() {
            Map<MatchKey, List<String>> keyMap = new TreeMap<>();
            keyMap.put(MatchKey.Name, Arrays.asList("Name"));
            keyMap.put(MatchKey.Country, Arrays.asList("Country"));
            keyMap.put(MatchKey.State, Arrays.asList("State"));
            keyMap.put(MatchKey.City, Arrays.asList("City"));
            keyMap.put(MatchKey.Zipcode, Arrays.asList("Zipcode"));
            keyMap.put(MatchKey.PhoneNumber, Arrays.asList("PhoneNumber"));
            return keyMap;
        }

        private List<String> getFields() {
            List<String> fields = Arrays.asList("Name", "Country", "State", "City", "Zipcode", "PhoneNumber");
            return fields;
        }
    }

}
