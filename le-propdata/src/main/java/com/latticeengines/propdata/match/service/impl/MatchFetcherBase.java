package com.latticeengines.propdata.match.service.impl;

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
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.propdata.core.datasource.DataSourceService;

public abstract class MatchFetcherBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(MatchFetcherBase.class);
    private LoadingCache<String, Set<String>> tableColumnsCache;

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

    protected MatchContext fetchSync(MatchContext context) {
        Map<String, List<Map<String, Object>>> resultMap = new HashMap<>();
        Map<String, List<String>> sourceColumnsMap = context.getSourceColumnsMap();
        for (Map.Entry<String, List<String>> sourceColumns : sourceColumnsMap.entrySet()) {
            QueryExecutor queryExecutor = getQueryExecutor(sourceColumns.getKey(), sourceColumns.getValue(), context);
            List<Map<String, Object>> queryResult = queryExecutor.query();
            resultMap.put(sourceColumns.getKey(), queryResult);
        }
        context.setResultsBySource(resultMap);
        return context;
    }

    private QueryExecutor getQueryExecutor(String sourceName, List<String> targetColumns, MatchContext matchContext) {
        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
        if (MatchConstants.MODEL.equals(sourceName) || MatchConstants.DERIVED_COLUMNS.equals(sourceName)) {
            CachedMatchQueryExecutor callable = new CachedMatchQueryExecutor("Domain", "Name", "Country", "State", "City");
            callable.setSourceName(sourceName);
            callable.setTargetColumns(targetColumns);
            callable.setJdbcTemplate(jdbcTemplate);
            callable.setDomainSet(matchContext.getDomains());
            callable.setNameLocationSet(matchContext.getNameLocations());
            return callable;
        } else {
            throw new UnsupportedOperationException("Only support match against predefined selection "
                    + ColumnSelection.Predefined.Model + " and " + ColumnSelection.Predefined.DerivedColumns + " now.");
        }
    }

    private class CachedMatchQueryExecutor extends QueryExecutor {

        private Log log = LogFactory.getLog(CachedMatchQueryExecutor.class);

        private String domainField;
        private String nameField;
        private String countryField;
        private String stateField;
        private String cityField;

        CachedMatchQueryExecutor(String domainField, String nameField, String countryField, String stateField,
                            String cityField) {
            this.domainField = domainField;
            this.nameField = nameField;
            this.countryField = countryField;
            this.stateField = stateField;
            this.cityField = cityField;
        }

        private Set<String> domainSet;
        private Set<NameLocation> nameLocationSet;

        public void setDomainSet(Set<String> domainSet) {
            this.domainSet = domainSet;
        }

        public void setNameLocationSet(Set<NameLocation> nameLocationSet) {
            this.nameLocationSet = nameLocationSet;
        }

        @Override
        public List<Map<String, Object>> query() {
            Long beforeQuerying = System.currentTimeMillis();
            log.debug("Querying SQL table " + getSourceTableName());
            List<Map<String, Object>> results = jdbcTemplate
                    .queryForList(constructSqlQuery(targetColumns, getSourceTableName(), domainSet, nameLocationSet));
            log.info("Retrieved " + results.size() + " results from SQL table " + getSourceTableName() + ". Duration="
                    + (System.currentTimeMillis() - beforeQuerying));
            return results;
        }

        private String constructSqlQuery(List<String> columns, String tableName, Collection<String> domains,
                                         Collection<NameLocation> nameLocations) {

            List<String> columnsToQuery = new ArrayList<>();
            Set<String> columnsInTable;
            try {
                columnsInTable = tableColumnsCache.get(tableName);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            for (String columnName : columns) {
                if (columnsInTable.contains(columnName.toLowerCase())) {
                    columnsToQuery.add(columnName);
                } else {
                    log.debug("Cannot find column " + columnName + " from table " + tableName);
                }
            }

            String sql = "SELECT " + "[" + nameField + "], " + "[" + countryField + "], " + "[" + stateField + "], "
                    + (cityField != null ? "[" + cityField + "], " : "")
                    + (columnsToQuery.isEmpty() ? "" : "[" + StringUtils.join(columnsToQuery, "], [") + "], ") + "["
                    + domainField + "] \n" + "FROM [" + tableName + "] WITH(NOLOCK) \n" + "WHERE [" + domainField
                    + "] IN ('" + StringUtils.join(domains, "', '") + "')\n";

            for (NameLocation nameLocation : nameLocations) {
                if (StringUtils.isNotEmpty(nameLocation.getName())) {
                    sql += " OR ( ";
                    sql += String.format("[%s] = '%s'", nameField, nameLocation.getName().replace("'", "''"));
                    if (StringUtils.isNotEmpty(nameLocation.getCountry())) {
                        sql += String.format(" AND [%s] = '%s'", countryField,
                                nameLocation.getCountry().replace("'", "''"));
                    }
                    if (StringUtils.isNotEmpty(nameLocation.getState())) {
                        sql += String.format(" AND [%s] = '%s'", stateField,
                                nameLocation.getState().replace("'", "''"));
                    }
                    if (cityField != null && StringUtils.isNotEmpty(nameLocation.getCity())) {
                        sql += String.format(" AND [%s] = '%s'", cityField, nameLocation.getCity().replace("'", "''"));
                    }
                    sql += " )\n";
                }
            }
            return sql;
        }
    }

    @SuppressWarnings("unused")
    private class DomainBasedQueryExecutor extends QueryExecutor {

        private Log log = LogFactory.getLog(DomainBasedQueryExecutor.class);

        private Set<String> domainSet;

        public void setDomainSet(Set<String> domainSet) {
            this.domainSet = domainSet;
        }

        @Override
        public List<Map<String, Object>> query() {
            Long beforeQuerying = System.currentTimeMillis();
            log.debug("Querying SQL table " + getSourceTableName());
            List<Map<String, Object>> results = jdbcTemplate
                    .queryForList(constructSqlQuery(targetColumns, getSourceTableName(), domainSet));
            log.info("Retrieved " + results.size() + " results from SQL table " + getSourceTableName() + ". Duration="
                    + (System.currentTimeMillis() - beforeQuerying));
            return results;
        }

        private String constructSqlQuery(List<String> columns, String tableName, Collection<String> domains) {
            return "SELECT [" + StringUtils.join(columns, "], [") + "Domain] \n" + "FROM [" + tableName
                    + "] WITH(NOLOCK) \n" + "WHERE [Domain] IN ('" + StringUtils.join(domains, "', '") + "')";
        }
    }

    private abstract static class QueryExecutor {

        protected String sourceName;
        protected List<String> targetColumns;
        protected JdbcTemplate jdbcTemplate;

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public void setTargetColumns(List<String> targetColumns) {
            this.targetColumns = targetColumns;
        }

        public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        protected String getSourceTableName() {
            if (MatchConstants.MODEL.equals(sourceName) || MatchConstants.DERIVED_COLUMNS.equals(sourceName)) {
                return MatchConstants.CACHE_TABLE;
            } else {
                return null;
            }
        }

        protected abstract List<Map<String, Object>> query();

    }



}
