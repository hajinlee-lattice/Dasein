package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.propdata.DataSourcePool;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.DisposableEmailService;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.PublicDomainService;

public abstract class MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);

    private static final Integer MAX_FETCH_THREADS = 32;
    private LoadingCache<String, Set<String>> tableColumnsCache;
    private ExecutorService executor = new ThreadPoolExecutor(1, MAX_FETCH_THREADS, 5, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(2 * MAX_FETCH_THREADS));

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private MetricService metricService;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private DisposableEmailService disposableEmailService;

    @PostConstruct
    private void postConstruct() {
        buildSourceColumnMapCache();
    }

    @MatchStep
    protected MatchContext fetch(MatchContext context) {
        Map<String, List<Map<String, Object>>> resultMap = new HashMap<>();
        Map<String, List<String>> sourceColumnsMap = context.getSourceColumnsMap();

        Map<String, Future<List<Map<String, Object>>>> futureMap = new HashMap<>();
        for (Map.Entry<String, List<String>> sourceColumns : sourceColumnsMap.entrySet()) {
            MatchCallable callable = getMatchCallable(sourceColumns.getKey(), sourceColumns.getValue(), context);
            Future<List<Map<String, Object>>> future = executor.submit(callable);
            futureMap.put(sourceColumns.getKey(), future);
        }
        for (Map.Entry<String, List<String>> sourceColumns : sourceColumnsMap.entrySet()) {
            Future<List<Map<String, Object>>> future = futureMap.get(sourceColumns.getKey());
            try {
                resultMap.put(sourceColumns.getKey(), future.get());
            } catch (InterruptedException | ExecutionException e) {
                logError(log, "Failed to fetch data from " + sourceColumns.getKey(), context, e);
                throw new RuntimeException("Failed to fetch data from " + sourceColumns.getKey(), e);
            }
        }

        context.setResultsBySource(resultMap);
        return context;
    }

    @MatchStep
    protected MatchContext complete(MatchContext matchContext) {
        List<InternalOutputRecord> internalOutputRecords = distributeResults(matchContext.getInternalResults(),
                matchContext.getResultsBySource());
        matchContext.setInternalResults(internalOutputRecords);
        matchContext = mergeResults(matchContext);
        matchContext.getOutput().setFinishedAt(new Date());
        Long receiveTime = matchContext.getOutput().getReceivedAt().getTime();
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - receiveTime);
        return matchContext;
    }

    @VisibleForTesting
    List<InternalOutputRecord> distributeResults(List<InternalOutputRecord> records,
            Map<String, List<Map<String, Object>>> resultsMap) {
        for (Map.Entry<String, List<Map<String, Object>>> result : resultsMap.entrySet()) {
            String sourceName = result.getKey();
            if (isCachedSource(sourceName)) {
                distributeCachedSourceResults(records, sourceName, result.getValue());
            } else if (isDomainSource(sourceName)) {
                String domainField = getDomainField(sourceName);
                for (InternalOutputRecord record : records) {
                    String parsedDomain = record.getParsedDomain();
                    if (StringUtils.isEmpty(parsedDomain) || publicDomainService.isPublicDomain(parsedDomain)) {
                        continue;
                    }

                    for (Map<String, Object> row : result.getValue()) {
                        if (row.containsKey(domainField) && row.get(domainField).equals(parsedDomain)) {
                            record.getResultsInSource().put(sourceName, row);
                        }
                    }
                }
            }
        }
        return records;
    }

    private void distributeCachedSourceResults(List<InternalOutputRecord> records, String sourceName,
            List<Map<String, Object>> rows) {
        String domainField = getDomainField(sourceName);
        String nameField = getNameField(sourceName);
        String cityField = getCityField(sourceName);
        String stateField = getStateField(sourceName);
        String countryField = getCountryField(sourceName);
        for (InternalOutputRecord record : records) {
            if (record.isFailed()) {
                continue;
            }
            // try using domain first
            boolean matched = false;
            String parsedDomain = record.getParsedDomain();
            if (StringUtils.isNotEmpty(parsedDomain)) {
                for (Map<String, Object> row : rows) {
                    if (row.containsKey(domainField) && parsedDomain.equals(row.get(domainField))) {
                        record.getResultsInSource().put(sourceName, row);
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
                            if (row.get(nameField) == null
                                    || !parsedName.equalsIgnoreCase((String) row.get(nameField))) {
                                continue;
                            }

                            Object countryInRow = row.get(countryField);
                            Object stateInRow = row.get(stateField);
                            Object cityInRow = row.get(cityField);

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

                            record.getResultsInSource().put(sourceName, row);
                            break;
                        }
                    }
                }

            }
        }
    }

    @VisibleForTesting
    @MatchStep
    MatchContext mergeResults(MatchContext matchContext) {
        List<InternalOutputRecord> records = matchContext.getInternalResults();
        List<String> outputFields = matchContext.getOutput().getOutputFields();
        Map<String, List<String>> columnPriorityMap = matchContext.getColumnPriorityMap();
        boolean returnUnmatched = matchContext.isReturnUnmatched();

        List<OutputRecord> outputRecords = new ArrayList<>();
        Integer[] columnMatchCount = new Integer[outputFields.size()];
        for (int i = 0; i < outputFields.size(); i++) {
            columnMatchCount[i] = 0;
        }

        int totalMatched = 0;

        for (InternalOutputRecord internalRecord : records) {
            if (internalRecord.isFailed()) {
                OutputRecord outputRecord = new OutputRecord();
                outputRecord.setInput(internalRecord.getInput());
                outputRecord.setMatched(false);
                outputRecord.setRowNumber(internalRecord.getRowNumber());
                outputRecord.setErrorMessages(internalRecord.getErrorMessages());
                outputRecords.add(outputRecord);
                continue;
            }

            internalRecord.setColumnMatched(new ArrayList<Boolean>());
            List<Object> output = new ArrayList<>();
            Map<String, Map<String, Object>> results = internalRecord.getResultsInSource();
            boolean matchedAnyColumn = false;
            for (int i = 0; i < outputFields.size(); i++) {
                String field = outputFields.get(i);
                Object value = null;
                boolean matched = false;

                if (MatchConstants.IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)
                        && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                        && publicDomainService.isPublicDomain(internalRecord.getParsedDomain())) {
                    matched = true;
                    value = true;
                } else if (MatchConstants.DISPOSABLE_EMAIL.equalsIgnoreCase(field)
                        && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                        && disposableEmailService.isDisposableEmailDomain(internalRecord.getParsedDomain())) {
                    matched = true;
                    value = true;
                } else if (columnPriorityMap.containsKey(field)) {
                    for (String targetSource : columnPriorityMap.get(field)) {
                        if (results.containsKey(targetSource)) {
                            matched = true;
                            Object objInResult = results.get(targetSource).get(field);
                            value = (objInResult == null ? value : objInResult);
                        }
                    }
                }

                output.add(value);

                if (ColumnSelection.Predefined.Model.equals(matchContext.getInput().getPredefinedSelection())
                        || ColumnSelection.Predefined.DerivedColumns
                                .equals(matchContext.getInput().getPredefinedSelection())) {
                    columnMatchCount[i] += (value == null ? 0 : 1);
                    internalRecord.getColumnMatched().add(value != null);
                } else {
                    columnMatchCount[i] += (matched ? 1 : 0);
                    internalRecord.getColumnMatched().add(matched);
                }

                matchedAnyColumn = matchedAnyColumn || matched;
            }

            // make *_IsMatched columns not null
            if (matchedAnyColumn) {
                for (int i = 0; i < outputFields.size(); i++) {
                    String field = outputFields.get(i);
                    Object value = output.get(i);
                    if (field.toLowerCase().contains("ismatched") && value == null) {
                        output.set(i, false);
                    }
                    if (MatchConstants.IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)) {
                        output.set(i, publicDomainService.isPublicDomain(internalRecord.getParsedDomain()));
                    }
                    if (MatchConstants.DISPOSABLE_EMAIL.equalsIgnoreCase(field)) {
                        output.set(i, disposableEmailService.isDisposableEmailDomain(internalRecord.getParsedDomain()));
                    }
                }
            }

            internalRecord.setOutput(output);

            if (matchedAnyColumn) {
                totalMatched++;
                internalRecord.setMatched(true);
            } else {
                if (internalRecord.getErrorMessages() == null) {
                    internalRecord.setErrorMessages(new ArrayList<String>());
                }
                internalRecord.getErrorMessages().add("The input does not match to any source.");
            }

            if (returnUnmatched || matchedAnyColumn) {
                internalRecord.setResultsInSource(null);
                OutputRecord outputRecord = new OutputRecord();
                if (matchedAnyColumn) {
                    outputRecord.setOutput(internalRecord.getOutput());
                }
                outputRecord.setInput(internalRecord.getInput());
                outputRecord.setMatched(internalRecord.isMatched());
                outputRecord.setMatchedDomain(internalRecord.getParsedDomain());
                outputRecord.setMatchedNameLocation(internalRecord.getParsedNameLocation());
                outputRecord.setRowNumber(internalRecord.getRowNumber());
                outputRecord.setErrorMessages(internalRecord.getErrorMessages());
                outputRecords.add(outputRecord);
            }
        }

        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(totalMatched);
        matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));

        return matchContext;
    }

    @Override
    public MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection.Predefined selection) {
        if (ColumnSelection.Predefined.Model.equals(selection)
                || ColumnSelection.Predefined.DerivedColumns.equals(selection)) {
            List<ColumnMetadata> metadata = columnMetadataService.fromPredefinedSelection(selection);
            matchOutput.setMetadata(metadata);
            return matchOutput;
        }
        throw new RuntimeException("Cannot find the requested metadata.");
    }

    private boolean isDomainSource(String sourceName) {
        return true;
    }

    private boolean isCachedSource(String sourceName) {
        return MatchConstants.MODEL.equals(sourceName) || MatchConstants.DERIVED_COLUMNS.equals(sourceName);
    }

    private String getDomainField(String sourceName) {
        return "Domain";
    }

    private String getNameField(String sourceName) {
        return "Name";
    }

    private String getCityField(String sourceName) {
        return "City";
    }

    private String getStateField(String sourceName) {
        return "State";
    }

    private String getCountryField(String sourceName) {
        return "Country";
    }

    private MatchCallable getMatchCallable(String sourceName, List<String> targetColumns, MatchContext matchContext) {
        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
        if (MatchConstants.MODEL.equals(sourceName) || MatchConstants.DERIVED_COLUMNS.equals(sourceName)) {
            CachedMatchCallable callable = new CachedMatchCallable("Domain", "Name", "Country", "State", "City");
            callable.setSourceName(sourceName);
            callable.setTargetColumns(targetColumns);
            callable.setJdbcTemplate(jdbcTemplate);
            callable.setDomainSet(matchContext.getDomains());
            callable.setNameLocationSet(matchContext.getNameLocations());
            callable.setRootOperationUID(matchContext.getOutput().getRootOperationUID());
            return callable;
        } else {
            throw new UnsupportedOperationException("Only support match against predefined selection "
                    + ColumnSelection.Predefined.Model + " and " + ColumnSelection.Predefined.DerivedColumns + " now.");
        }
    }

    private class CachedMatchCallable extends MatchCallable implements Callable<List<Map<String, Object>>> {

        private Log log = LogFactory.getLog(CachedMatchCallable.class);

        private String domainField;
        private String nameField;
        private String countryField;
        private String stateField;
        private String cityField;

        CachedMatchCallable(String domainField, String nameField, String countryField, String stateField,
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
        public List<Map<String, Object>> call() {
            Long beforeQuerying = System.currentTimeMillis();
            log.info("Querying SQL table " + getSourceTableName() + " RootOperationUID=" + rootOperationUID);
            List<Map<String, Object>> results = jdbcTemplate
                    .queryForList(constructSqlQuery(targetColumns, getSourceTableName(), domainSet, nameLocationSet));
            log.info("Retrieved " + results.size() + " results from SQL table " + getSourceTableName() + ". Duration="
                    + (System.currentTimeMillis() - beforeQuerying) + " RootOperationUID=" + rootOperationUID);
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
                    log.warn("Cannot find column " + columnName + " from table " + tableName);
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
    private class DomainBasedMatchCallable extends MatchCallable implements Callable<List<Map<String, Object>>> {

        private Log log = LogFactory.getLog(DomainBasedMatchCallable.class);

        private Set<String> domainSet;

        public void setDomainSet(Set<String> domainSet) {
            this.domainSet = domainSet;
        }

        @Override
        public List<Map<String, Object>> call() {
            Long beforeQuerying = System.currentTimeMillis();
            log.info("Querying SQL table " + getSourceTableName() + " RootOperationUID=" + rootOperationUID);
            List<Map<String, Object>> results = jdbcTemplate
                    .queryForList(constructSqlQuery(targetColumns, getSourceTableName(), domainSet));
            log.info("Retrieved " + results.size() + " results from SQL table " + getSourceTableName() + ". Duration="
                    + (System.currentTimeMillis() - beforeQuerying) + " RootOperationUID=" + rootOperationUID);
            return results;
        }

        private String constructSqlQuery(List<String> columns, String tableName, Collection<String> domains) {
            return "SELECT [" + StringUtils.join(columns, "], [") + "Domain] \n" + "FROM [" + tableName
                    + "] WITH(NOLOCK) \n" + "WHERE [Domain] IN ('" + StringUtils.join(domains, "', '") + "')";
        }
    }

    private abstract static class MatchCallable implements Callable<List<Map<String, Object>>> {

        protected String sourceName;
        protected List<String> targetColumns;
        protected JdbcTemplate jdbcTemplate;
        protected String rootOperationUID;

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public void setTargetColumns(List<String> targetColumns) {
            this.targetColumns = targetColumns;
        }

        public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        protected void setRootOperationUID(String rootOperationUID) {
            this.rootOperationUID = rootOperationUID;
        }

        protected String getSourceTableName() {
            if (MatchConstants.MODEL.equals(sourceName) || MatchConstants.DERIVED_COLUMNS.equals(sourceName)) {
                return MatchConstants.CACHE_TABLE;
            } else {
                return null;
            }
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

    private static void logError(Log log, String message, MatchContext matchContext, Exception e) {
        if (e == null) {
            log.error(message + " RootOperationUID=" + matchContext.getOutput().getRootOperationUID());
        } else {
            log.error(message + " RootOperationUID=" + matchContext.getOutput().getRootOperationUID(), e);
        }
    }

}
