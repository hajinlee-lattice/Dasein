package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.core.datasource.DataSourceService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.DomainCollectService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.newrelic.api.agent.Trace;

@Component("sqlServerHelper")
public class SqlServerHelper implements DbHelper {

    private static final Log log = LogFactory.getLog(SqlServerHelper.class);
    private LoadingCache<String, Set<String>> tableColumnsCache;
    private static final Integer MAX_RETRIES = 2;

    private static final Integer QUEUE_SIZE = 20000;
    private static final Integer TIMEOUT_MINUTE = 10;

    private final BlockingQueue<MatchContext> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final ConcurrentMap<String, MatchContext> map = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> fetcherActivity = new ConcurrentHashMap<>();
    private ExecutorService executor;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    @Qualifier("columnSelectionService")
    private ColumnSelectionService columnSelectionService;

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
    private DomainCollectService domainCollectService;

    private boolean fetchersInitiated = false;

    @PostConstruct
    private void postConstruct() {
        buildSourceColumnMapCache();
        if (enableFetchers) {
            initExecutors();
        }
    }

    private void buildSourceColumnMapCache() {
        tableColumnsCache = CacheBuilder.newBuilder().concurrencyLevel(4).weakKeys()
                .expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<String, Set<String>>() {
                    public Set<String> load(String key) {
                        JdbcTemplate jdbcTemplate = dataSourceService
                                .getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
                        List<String> columnsByQuery = jdbcTemplate
                                .queryForList(
                                        "SELECT COLUMN_NAME " + "FROM INFORMATION_SCHEMA.COLUMNS\n"
                                                + "WHERE TABLE_NAME = '" + key + "' AND TABLE_SCHEMA='dbo'",
                                        String.class);
                        Set<String> columnsInSql = new HashSet<>();
                        for (String columnName : columnsByQuery) {
                            columnsInSql.add(columnName.toLowerCase());
                        }
                        return columnsInSql;
                    }
                });
    }

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    public MatchContext sketchExecutionPlan(MatchContext matchContext, boolean skipExecutionPlanning) {
        if (!skipExecutionPlanning) {
            ColumnSelection columnSelection = matchContext.getColumnSelection();
            matchContext.setPartitionColumnsMap(columnSelectionService.getPartitionColumnMap(columnSelection));
        }
        return matchContext;
    }

    @Override
    public MatchContext fetch(MatchContext matchContext) {
        if (enableFetchers) {
            queue.add(matchContext);
            return waitForResult(matchContext.getOutput().getRootOperationUID());
        } else {
            return fetchSync(matchContext);
        }
    }

    @Override
    public MatchContext fetchSync(MatchContext context) {
        if (context.getDomains().isEmpty() && context.getNameLocations().isEmpty()) {
            log.info("Noting to fetch.");
            context.setResultSet(Collections.emptyList());
            return context;
        }

        Map<String, Set<String>> partitionColumnsMap = context.getPartitionColumnsMap();

        Set<String> involvedPartitions = new HashSet<>(partitionColumnsMap.keySet());
        involvedPartitions.add(MatchConstants.CACHE_TABLE);

        Set<String> targetColumns = new HashSet<>();
        for (Map.Entry<String, Set<String>> partitionColumns : partitionColumnsMap.entrySet()) {
            String tableName = partitionColumns.getKey();
            try {
                Set<String> columnsInTable = new HashSet<>(tableColumnsCache.get(tableName));
                for (String columnName : partitionColumns.getValue()) {
                    if (columnsInTable.contains(columnName.toLowerCase())) {
                        targetColumns.add(columnName);
                    } else {
                        log.debug("Cannot find column " + columnName + " from table " + tableName);
                    }
                }
            } catch (ExecutionException e) {
                throw new RuntimeException("Cannot verify columns in table " + tableName, e);
            }
        }

        String sql;
        if (context.getInput().getFetchOnly()) {
            Set<String> latticeAccountIds = new HashSet<>();
            for (InternalOutputRecord record : context.getInternalResults()) {
                if (StringUtils.isNotEmpty(record.getLatticeAccountId())) {
                    latticeAccountIds.add(record.getLatticeAccountId());
                }
            }
            sql = constructSqlQueryForFetching(involvedPartitions, targetColumns, latticeAccountIds);
        } else {
            sql = constructSqlQuery(involvedPartitions, targetColumns, context.getDomains(),
                    context.getNameLocations());
        }

        List<JdbcTemplate> jdbcTemplates = dataSourceService.getJdbcTemplatesFromDbPool(DataSourcePool.SourceDB,
                MAX_RETRIES);
        for (JdbcTemplate jdbcTemplate : jdbcTemplates) {
            try {
                List<Map<String, Object>> queryResult = query(jdbcTemplate, sql);
                context.setResultSet(queryResult);
                break;
            } catch (Exception e) {
                log.error("Attempt to execute query failed.", e);
            }
        }

        // send to collector
        for  (String domain: context.getDomains()) {
            domainCollectService.enqueue(domain);
        }

        return context;
    }

    @Override
    public void fetchIdResult(MatchContext context) {
    }

    @Override
    public List<MatchContext> fetch(List<MatchContext> contexts) {
        if (contexts.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("Enter executeBulk for " + contexts.size() + " match contexts.");

        List<String> rootUids = enqueue(contexts);
        return waitForResult(rootUids);
    }

    @Override
    public MatchContext updateInternalResults(MatchContext context) {
        List<InternalOutputRecord> internalOutputRecords = distributeResults(context.getInternalResults(),
                context.getResultSet());
        context.setInternalResults(internalOutputRecords);
        return context;
    }

    @Override
    public MatchContext mergeContexts(List<MatchContext> matchContextList, String dataCloudVersion) {
        MatchContext mergedContext = new MatchContext();
        MatchInput dummyInput = new MatchInput();
        dummyInput.setDataCloudVersion(dataCloudVersion);
        mergedContext.setInput(dummyInput);

        Set<String> domainSet = new HashSet<>();
        Set<NameLocation> nameLocationSet = new HashSet<>();
        Map<String, Set<String>> srcColSetMap = new HashMap<>();

        for (MatchContext matchContext : matchContextList) {
            domainSet.addAll(matchContext.getDomains());
            nameLocationSet.addAll(matchContext.getNameLocations());
            Map<String, Set<String>> srcColMap1 = matchContext.getPartitionColumnsMap();
            for (Map.Entry<String, Set<String>> entry : srcColMap1.entrySet()) {
                String sourceName = entry.getKey();
                if (srcColSetMap.containsKey(sourceName)) {
                    srcColSetMap.get(sourceName).addAll(entry.getValue());
                } else {
                    srcColSetMap.put(sourceName, new HashSet<>(entry.getValue()));
                }
            }
        }
        Map<String, Set<String>> srcColMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : srcColSetMap.entrySet()) {
            srcColMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        mergedContext.setDomains(domainSet);
        mergedContext.setNameLocations(nameLocationSet);
        mergedContext.setPartitionColumnsMap(srcColMap);
        return mergedContext;
    }

    @Override
    public void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList) {
        List<Map<String, Object>> resultSet = mergedContext.getResultSet();
        for (MatchContext context : matchContextList) {
            context.setResultSet(resultSet);
        }
    }

    @Override
    public MatchContext fetchAsync(MatchContext context) {
        return context;
    }

    @Override
    public void fetchMatchResult(MatchContext context) {
    }

    private String constructSqlQuery(Set<String> involvedPartitions, Set<String> targetColumns,
            Collection<String> domains, Collection<NameLocation> nameLocations) {

        String sql = String.format("SELECT p1.[%s], p1.[%s], p1.[%s], p1.[%s], p1.[%s], p1.[%s]",
                MatchConstants.LID_FIELD, MatchConstants.DOMAIN_FIELD, MatchConstants.NAME_FIELD,
                MatchConstants.COUNTRY_FIELD, MatchConstants.STATE_FIELD, MatchConstants.CITY_FIELD);
        sql += (targetColumns.isEmpty() ? "" : ", [" + StringUtils.join(targetColumns, "], [") + "]");
        sql += "\nFROM " + fromJoinClause(involvedPartitions);
        sql += "\nWHERE p1.[" + MatchConstants.DOMAIN_FIELD + "] IN ('" + StringUtils.join(domains, "', '") + "')\n";

        for (NameLocation nameLocation : nameLocations) {
            if (StringUtils.isEmpty(nameLocation.getCountry())) {
                nameLocation.setCountry(LocationUtils.USA);
            }
            if (StringUtils.isNotEmpty(nameLocation.getName()) && StringUtils.isNotEmpty(nameLocation.getState())) {
                sql += " OR ( ";
                sql += String.format("p1.[%s] = '%s'", MatchConstants.NAME_FIELD,
                        nameLocation.getName().replace("'", "''"));
                if (StringUtils.isNotEmpty(nameLocation.getCountry())) {
                    sql += String.format(" AND p1.[%s] = '%s'", MatchConstants.COUNTRY_FIELD,
                            nameLocation.getCountry().replace("'", "''"));
                }
                if (StringUtils.isNotEmpty(nameLocation.getState())) {
                    sql += String.format(" AND p1.[%s] = '%s'", MatchConstants.STATE_FIELD,
                            nameLocation.getState().replace("'", "''"));
                }
                if (StringUtils.isNotEmpty(nameLocation.getCity())) {
                    sql += String.format(" AND p1.[%s] = '%s'", MatchConstants.CITY_FIELD,
                            nameLocation.getCity().replace("'", "''"));
                }
                sql += " )\n";
            }
        }
        return sql;
    }

    private String constructSqlQueryForFetching(Set<String> involvedPartitions, Set<String> targetColumns,
            Collection<String> latticeAccountIds) {
        String sql = String.format("SELECT p1.[%s]", MatchConstants.LID_FIELD);
        sql += (targetColumns.isEmpty() ? "" : ", [" + StringUtils.join(targetColumns, "], [") + "]");
        sql += "\nFROM " + fromJoinClause(involvedPartitions);
        sql += "\nWHERE p1.[" + MatchConstants.LID_FIELD + "] IN ('" + StringUtils.join(latticeAccountIds, "', '")
                + "')\n";
        return sql;
    }

    private String fromJoinClause(Set<String> partitions) {
        String clause = "[" + MatchConstants.CACHE_TABLE + "] p1 WITH(NOLOCK)";
        partitions.remove(MatchConstants.CACHE_TABLE);

        int p = 1;
        for (String partition : partitions) {
            p++;
            clause += String.format("\n INNER JOIN [%s] p%d WITH(NOLOCK) ON p1.[%s]=p%d.[%s]", partition, p,
                    MatchConstants.LID_FIELD, p, MatchConstants.LID_FIELD);
        }

        return clause;
    }

    private List<Map<String, Object>> query(JdbcTemplate jdbcTemplate, String sql) {
        Long beforeQuerying = System.currentTimeMillis();
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql);
        String url = "";
        try {
            DriverManagerDataSource dataSource = (DriverManagerDataSource) jdbcTemplate.getDataSource();
            url = dataSource.getUrl();
            url = url.substring(0, url.indexOf(";"));
        } catch (Exception e) {
            log.warn("Failed to get url from jdbc template");
        }
        log.info("Retrieved " + results.size() + " results from SQL Server. Duration="
                + (System.currentTimeMillis() - beforeQuerying) + " Rows=" + results.size() + " URL=" + url);
        return results;
    }

    private List<InternalOutputRecord> distributeResults(List<InternalOutputRecord> records,
            List<Map<String, Object>> results) {
        distributeQueryResults(records, results);
        return records;
    }

    private void distributeQueryResults(List<InternalOutputRecord> records, List<Map<String, Object>> rows) {
        distributeQueryResults(records, null, rows);
    }

    @Trace
    private void distributeQueryResults(List<InternalOutputRecord> records, String sourceName,
            List<Map<String, Object>> rows) {
        boolean singlePartitionMode = StringUtils.isEmpty(sourceName);

        for (InternalOutputRecord record : records) {
            if (record.isFailed()) {
                continue;
            }
            boolean matched = false;

            // try lattice account id first
            String latticeAccountId = record.getLatticeAccountId();
            if (StringUtils.isNotEmpty(latticeAccountId)) {
                for (Map<String, Object> row : rows) {
                    Object rawId = row.get(MatchConstants.LID_FIELD);
                    String strId = String.valueOf(rawId);
                    if (row.containsKey(MatchConstants.LID_FIELD) && latticeAccountId.equals(strId)) {
                        row.put(MatchConstants.LID_FIELD, strId);
                        if (singlePartitionMode) {
                            record.setQueryResult(row);
                        } else {
                            record.getResultsInPartition().put(sourceName, row);
                        }
                        matched = true;
                        break;
                    }
                }
            }

            // then domain
            if (!matched) {
                String parsedDomain = record.getParsedDomain();
                if (StringUtils.isNotEmpty(parsedDomain)) {
                    for (Map<String, Object> row : rows) {
                        if (row.containsKey(MatchConstants.DOMAIN_FIELD)
                                && parsedDomain.equals(row.get(MatchConstants.DOMAIN_FIELD))) {
                            if (singlePartitionMode) {
                                record.setQueryResult(row);
                            } else {
                                record.getResultsInPartition().put(sourceName, row);
                            }
                            matched = true;
                            break;
                        }
                    }
                }
            }

            // finally, name + location
            if (!matched) {
                NameLocation nameLocation = record.getParsedNameLocation();
                if (nameLocation != null) {
                    String parsedName = nameLocation.getName();
                    if (StringUtils.isEmpty(nameLocation.getCountry())) {
                        nameLocation.setCountry(LocationUtils.USA);
                    }
                    String parsedCountry = nameLocation.getCountry();
                    String parsedState = nameLocation.getState();
                    String parsedCity = nameLocation.getCity();
                    if (StringUtils.isNotEmpty(parsedName)) {
                        for (Map<String, Object> row : rows) {
                            if (row.get(MatchConstants.NAME_FIELD) == null
                                    || !parsedName.equalsIgnoreCase((String) row.get(MatchConstants.NAME_FIELD))) {
                                continue;
                            }

                            Object countryInRow = row.get(MatchConstants.COUNTRY_FIELD);
                            Object stateInRow = row.get(MatchConstants.STATE_FIELD);
                            Object cityInRow = row.get(MatchConstants.CITY_FIELD);

                            if (countryInRow != null && !parsedCountry.equalsIgnoreCase((String) countryInRow)) {
                                continue;
                            }

                            if (countryInRow == null && !LocationUtils.USA.equalsIgnoreCase(parsedCountry)) {
                                continue;
                            }

                            if (StringUtils.isNotEmpty(parsedState) && stateInRow != null
                                    && !parsedState.equalsIgnoreCase((String) stateInRow)) {
                                continue;
                            }

                            if (StringUtils.isNotEmpty(parsedCity) && cityInRow != null
                                    && !parsedCity.equalsIgnoreCase((String) cityInRow)) {
                                continue;
                            }

                            if (singlePartitionMode) {
                                record.setQueryResult(row);
                            } else {
                                record.getResultsInPartition().put(sourceName, row);
                            }

                            matched = true;

                            break;
                        }
                    }
                }

            }

            if (matched) {
                record.setMatched(true);
                setMatchedValues(record);
            }
        }
    }

    private void setMatchedValues(InternalOutputRecord record) {
        Long latticeAccountId = (Long) record.getQueryResult().get(MatchConstants.LID_FIELD);
        record.setLatticeAccountId(latticeAccountId == null ? null : String.valueOf(latticeAccountId));
        record.setMatchedDomain((String) record.getQueryResult().get(MatchConstants.DOMAIN_FIELD));
        String name = (String) record.getQueryResult().get(MatchConstants.NAME_FIELD);
        String city = (String) record.getQueryResult().get(MatchConstants.CITY_FIELD);
        String state = (String) record.getQueryResult().get(MatchConstants.STATE_FIELD);
        String country = (String) record.getQueryResult().get(MatchConstants.COUNTRY_FIELD);
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(name);
        nameLocation.setCity(city);
        nameLocation.setState(state);
        nameLocation.setCity(country);
        record.setMatchedNameLocation(nameLocation);
    }

    @Override
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
        enableFetchers = true;
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
                    log.warn("Error from fetcher.");
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
                    MatchContext mergedContext = mergeContexts(matchContextList, groupDataCloudVersion);
                    mergedContext = fetchSync(mergedContext);
                    splitContext(mergedContext, matchContextList);
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

    private List<String> enqueue(List<MatchContext> matchContexts) {
        List<String> rootUids = new ArrayList<>(matchContexts.size());

        if (enableFetchers) {
            queue.addAll(matchContexts);
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

    private List<MatchContext> waitForResult(List<String> rootUids) {
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
