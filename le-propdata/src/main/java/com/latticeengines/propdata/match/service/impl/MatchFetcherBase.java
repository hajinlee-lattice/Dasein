package com.latticeengines.propdata.match.service.impl;

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
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.propdata.core.datasource.DataSourceService;

public abstract class MatchFetcherBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(MatchFetcherBase.class);
    private LoadingCache<String, Set<String>> tableColumnsCache;
    private static final Integer MAX_RETRIES = 2;

    @Autowired
    private DataSourceService dataSourceService;

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

    MatchContext fetchSync(MatchContext context) {
        Map<String, List<Map<String, Object>>> resultMap = new HashMap<>();
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
        log.info("Retrieved " + results.size() + " results from SQL Server. Duration="
                + (System.currentTimeMillis() - beforeQuerying));
        return results;
    }

}
