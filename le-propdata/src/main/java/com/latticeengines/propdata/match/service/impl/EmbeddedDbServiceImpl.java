package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.match.service.EmbeddedDbService;

@Component("embeddedDbService")
public class EmbeddedDbServiceImpl implements EmbeddedDbService {

    private static final Log log = LogFactory.getLog(EmbeddedDbServiceImpl.class);
    private static final String CACHE_TABLE = "DerivedColumnsCache";
    private static final AtomicInteger MIN_ID = new AtomicInteger();
    private static final AtomicInteger MAX_ID = new AtomicInteger();
    private static final AtomicInteger COUNT = new AtomicInteger();
    private static final AtomicInteger CAPACITY = new AtomicInteger();
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    static final String MATCH_KEY_CACHE = "MatchKeyCache";
    static final Integer UNAVAILABLE = 0;
    static final Integer LOADING = 1;
    static final Integer READY = 2;
    static final AtomicInteger STATUS = new AtomicInteger(UNAVAILABLE);

    @Autowired
    @Qualifier("embeddedJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier("matchExecutor")
    private ThreadPoolTaskExecutor matchExecutor;

    @Value("${propdata.embedded.chunk.size:10000}")
    private Integer chunckSize;

    @Value("${propdata.embedded.num.threads:16}")
    private Integer numThreads;

    @Autowired
    private DataSourceService dataSourceService;

    @PostConstruct
    private void postConstruct() {
        createMatchKeyCacheTable();
    }

    private void createMatchKeyCacheTable() {
        log.info("Creating table MatchKeyCache");
        jdbcTemplate.execute("CREATE TABLE " + MATCH_KEY_CACHE + " (" + "LatticeAccountID INTEGER PRIMARY KEY,"
                + "Domain VARCHAR_IGNORECASE(250)," + "Name VARCHAR_IGNORECASE(1000)," + "City VARCHAR_IGNORECASE(200),"
                + "State VARCHAR_IGNORECASE(200)," + "Country VARCHAR_IGNORECASE(100),"
                + "DUNS VARCHAR_IGNORECASE(10))");
    }

    @Override
    public void loadAsync() {
        matchExecutor.execute(new Runnable() {
            @Override
            public void run() {
                loadSync(-1);
            }
        });
    }

    @Override
    public Lock readLock() { return lock.readLock(); }

    @Override
    public Boolean isReady() {
        return READY.equals(STATUS.get());
    }

    @Override
    public void invalidate() {
        STATUS.set(UNAVAILABLE);
    }

    private Boolean isLoading() {
        return LOADING.equals(STATUS.get());
    }

    @VisibleForTesting
    void setChunckSize(Integer chunckSize) {
        this.chunckSize = chunckSize;
    }

    @VisibleForTesting
    void setNumThreads(Integer numThreads) {
        this.numThreads = numThreads;
    }

    /**
     * This is for testing purpose only. It will load more than required. The
     * purpose is just to not to load the whole 10+ M keys in test.
     * 
     * @param numRows
     */
    @VisibleForTesting
    synchronized void loadSync(Integer numRows) {
        if (isLoading()) {
            log.warn("Already loading embedded db.");
            return;
        }

        try {
            lock.writeLock().tryLock(10, TimeUnit.MINUTES);
            log.info("Set status to LOADING.");
            STATUS.set(LOADING);

            loadInternal(numRows);

            if (COUNT.get() >= CAPACITY.get() && LOADING.equals(STATUS.get())) {
                log.info("Set status to READY.");
                STATUS.set(READY);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to start the loading process.", e);
        } finally {
            if (!READY.equals(STATUS.get())) {
                log.info("Set status to UNAVAILABLE.");
                STATUS.set(UNAVAILABLE);
            }

            lock.writeLock().unlock();
        }
    }

    private void loadInternal(Integer numRows) {

        truncateMatchKeyCache();

        JdbcTemplate sourceJdbc = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);

        if (numRows == null || numRows <= 0) {
            numRows = sourceJdbc.queryForObject("SELECT COUNT(*) FROM " + CACHE_TABLE + " WITH(NOLOCK)", Integer.class);
            log.info("Loading all " + numRows + " match keys to " + MATCH_KEY_CACHE + " from SourceDB.");
        } else {
            log.info("Loading " + numRows + " samples to " + MATCH_KEY_CACHE + " from SourceDB.");
        }

        loadMinMaxId(sourceJdbc);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        CAPACITY.set(numRows);
        COUNT.set(0);

        Integer startId = MIN_ID.get();
        Integer endId;
        List<Future<String>> futures = new ArrayList<>();
        while (startId < MAX_ID.get() && COUNT.get() < numRows && LOADING.equals(STATUS.get())) {
            if (futures.size() >= numThreads) {
                unloadFutures(futures);
                futures = new ArrayList<>();
            }
            endId = startId + chunckSize - 1;
            JdbcTemplate jdbc = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
            Future<String> future = executor.submit(new QueryingCallable(jdbc, startId, endId));
            futures.add(future);
            startId = endId + 1;
        }

        if (!futures.isEmpty()) {
            unloadFutures(futures);
        }

        log.info("Finished. Loaded " + COUNT.get() + " out of " + CAPACITY.get() + " rows in total. Current Status is LOADING = "
                + LOADING.equals(STATUS.get()));
        executor.shutdown();
    }

    private void unloadFutures (List<Future<String>> futures) {
        for (Future<String> future : futures) {
            try {
                String sql = future.get();
                if (StringUtils.isNotEmpty(sql)) {
                    jdbcTemplate.execute(sql);
                    Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + MATCH_KEY_CACHE,
                            Integer.class);
                    log.info("Loaded " + count + " rows.");
                    COUNT.set(count);
                    if (count >= CAPACITY.get() || !LOADING.equals(STATUS.get())) {
                        break;
                    }
                }
            } catch (InterruptedException|ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class QueryingCallable implements Callable<String> {
        private final Integer startId;
        private final Integer endId;
        private final JdbcTemplate sourceJdbc;

        QueryingCallable(JdbcTemplate sourceJdbc, Integer startId, Integer endId) {
            this.startId = startId;
            this.endId = endId;
            this.sourceJdbc = sourceJdbc;
        }

        @Override
        public String call() {
            if (COUNT.get() >= CAPACITY.get() || !LOADING.equals(STATUS.get())) {
                return null;
            }

            try {
                return loadFromSourceDB();
            } catch (Exception e) {
                log.error(e);
                return null;
            }
        }

        private String loadFromSourceDB() {
            List<Map<String, Object>> results = sourceJdbc.queryForList(String.format(
                    "SELECT TOP %d "
                            + "[LatticeAccountID], [Domain], [BusinessName], [BusinessCity], [BusinessState], [BusinessCountry] "
                            + "FROM %s WITH(NOLOCK) WHERE [LatticeAccountID] >= %d AND [LatticeAccountID] <= %d",
                    chunckSize, CACHE_TABLE, startId, endId));
            log.info("Received " + results.size() + " tuples between " + startId + " and " + endId + ".");

            String sql = "INSERT INTO " + MATCH_KEY_CACHE
                    + " (LatticeAccountID, Domain, Name, City, State, Country) VALUES \n";
            List<String> values = new ArrayList<>();
            for (Map<String, Object> result : results) {
                List<String> fields = new ArrayList<>();
                fields.add(String.valueOf(result.get("LatticeAccountID")));
                fields.add(getStringValueFromMap(result, "Domain"));
                fields.add(getStringValueFromMap(result, "BusinessName"));
                fields.add(getStringValueFromMap(result, "BusinessCity"));
                fields.add(getStringValueFromMap(result, "BusinessState"));
                fields.add(getStringValueFromMap(result, "BusinessCountry"));
                String value = "(" + StringUtils.join(fields, ",") + ")";
                values.add(value);
            }
            sql += StringUtils.join(values, ",\n");
            return sql;
        }

    }

    private void loadMinMaxId(JdbcTemplate sourceJdbc) {
        synchronized (MIN_ID) {
            Integer minId = sourceJdbc.queryForObject(
                    "SELECT MIN(LatticeAccountID) FROM " + CACHE_TABLE + " WITH(NOLOCK)", Integer.class);
            if (minId == null) {
                throw new RuntimeException("Cannot find min LatticeAccountID.");
            }
            MIN_ID.set(minId);
        }
        synchronized (MAX_ID) {
            Integer maxId = sourceJdbc.queryForObject(
                    "SELECT MAX(LatticeAccountID) FROM " + CACHE_TABLE + " WITH(NOLOCK)", Integer.class);
            if (maxId == null) {
                throw new RuntimeException("Cannot find max LatticeAccountID.");
            }
            MAX_ID.set(maxId);
        }
        log.info("MinID=" + MIN_ID.get() + ", MaxID=" + MAX_ID.get());
    }

    private String getStringValueFromMap(Map<String, Object> result, String col) {
        Object obj = null;
        if (result.containsKey(col)) {
            obj = result.get(col);
        }
        return (obj == null) ? "null" : "'" + String.valueOf(obj).replace("'", "''") + "'";
    }

    private void truncateMatchKeyCache() {
        jdbcTemplate.execute("TRUNCATE TABLE " + MATCH_KEY_CACHE);
    }

}
