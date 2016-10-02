package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.domain.exposed.datacloud.DataSourcePool;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.newrelic.api.agent.Trace;

@Component("sqlServerHelper")
public class SqlServerHelper implements DbHelper {

    private static final Log log = LogFactory.getLog(SqlServerHelper.class);
    private LoadingCache<String, Set<String>> tableColumnsCache;
    private static final Integer MAX_RETRIES = 2;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    @Qualifier("columnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @PostConstruct
    private void postConstruct() {
        buildSourceColumnMapCache();
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
        return MatchTypeUtil.isValidForRTSBasedMatch(version);
    }

    @Override
    public void populateMatchHints(MatchContext context) {
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
    public MatchContext fetch(MatchContext context) {
        if (context.getDomains().isEmpty() && context.getNameLocations().isEmpty()) {
            log.info("Noting to fetch.");
            context.setResultSet(new ArrayList<Map<String, Object>>());
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

        String sql = constructSqlQuery(involvedPartitions, targetColumns, context.getDomains(),
                context.getNameLocations());
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
        return context;
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

    private String constructSqlQuery(Set<String> involvedPartitions, Set<String> targetColumns,
            Collection<String> domains, Collection<NameLocation> nameLocations) {

        String sql = String.format("SELECT p1.[%s], p1.[%s], p1.[%s], p1.[%s], p1.[%s]", MatchConstants.DOMAIN_FIELD,
                MatchConstants.NAME_FIELD, MatchConstants.COUNTRY_FIELD, MatchConstants.STATE_FIELD,
                MatchConstants.CITY_FIELD);
        sql += (targetColumns.isEmpty() ? "" : ", [" + StringUtils.join(targetColumns, "], [") + "]");
        sql += "\nFROM " + fromJoinClause(involvedPartitions);
        sql += "\nWHERE p1.[" + MatchConstants.DOMAIN_FIELD + "] IN ('" + StringUtils.join(domains, "', '") + "')\n";

        for (NameLocation nameLocation : nameLocations) {
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

    public List<Map<String, Object>> query(JdbcTemplate jdbcTemplate, String sql) {
        Long beforeQuerying = System.currentTimeMillis();
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql);
        String url = "";
        try {
            DriverManagerDataSource dataSource = (DriverManagerDataSource) jdbcTemplate.getDataSource();
            url = dataSource.getUrl();
            url = url.substring(0, url.indexOf(";"));
        } catch (Exception e) {
            log.warn("Failed to get url from jdbc template", e);
        }
        log.info("Retrieved " + results.size() + " results from SQL Server. Duration="
                + (System.currentTimeMillis() - beforeQuerying) + " Rows=" + results.size() + " URL=" + url);
        return results;
    }

    @VisibleForTesting
    List<InternalOutputRecord> distributeResults(List<InternalOutputRecord> records,
            Map<String, List<Map<String, Object>>> resultsMap) {
        for (Map.Entry<String, List<Map<String, Object>>> result : resultsMap.entrySet()) {
            String sourceName = result.getKey();
            distributeQueryResults(records, sourceName, result.getValue());
        }
        return records;
    }

    @VisibleForTesting
    List<InternalOutputRecord> distributeResults(List<InternalOutputRecord> records,
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
            // try using domain first
            boolean matched = false;
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

            if (!matched) {
                NameLocation nameLocation = record.getParsedNameLocation();
                if (nameLocation != null) {
                    String parsedName = nameLocation.getName();
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

                            break;
                        }
                    }
                }

            }
        }
    }
}
