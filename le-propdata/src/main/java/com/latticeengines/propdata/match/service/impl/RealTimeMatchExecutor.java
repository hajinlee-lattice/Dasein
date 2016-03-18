package com.latticeengines.propdata.match.service.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.monitor.metric.SqlQueryMetric;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyDimension;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.FetchDataFromSql;
import com.latticeengines.propdata.match.metric.MatchedAccount;
import com.latticeengines.propdata.match.metric.MatchedColumn;
import com.latticeengines.propdata.match.metric.RealTimeResponse;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.PublicDomainService;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);
    private static final String CACHE_TABLE = "DerivedColumnsCache";
    private static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    private static final String MODEL = ColumnSelection.Predefined.Model.getName();
    private static final Integer MAX_FETCH_THREADS = 4;

    private LoadingCache<String, Set<String>> tableColumnsCache;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private MetricService metricService;

    @Autowired
    private PublicDomainService publicDomainService;

    @PostConstruct
    private void postConstruct() {
        buildSourceColumnMapCache();
    }

    @Override
    public MatchContext executeMatch(MatchContext matchContext) {
        matchContext = fetch(matchContext);
        matchContext = complete(matchContext);
        generateOutputMetrics(matchContext);
        return matchContext;
    }

    @MatchStep
    private MatchContext fetch(MatchContext context) {
        context.setStatus(MatchStatus.FETCHING);

        Map<String, List<Map<String, Object>>> resultMap = new HashMap<>();
        Map<String, List<String>> sourceColumnsMap = context.getSourceColumnsMap();

        ExecutorService executor;
        if (sourceColumnsMap.size() <= MAX_FETCH_THREADS) {
            executor = Executors.newCachedThreadPool();
        } else {
            executor = Executors.newFixedThreadPool(MAX_FETCH_THREADS);
        }
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
                LoggingUtils.logError(log, "Failed to fetch data from " + sourceColumns.getKey(), context, e);
                throw new RuntimeException("Failed to fetch data from " + sourceColumns.getKey(), e);
            }
        }

        context.setResultsBySource(resultMap);
        context.setStatus(MatchStatus.FETCHED);
        return context;
    }

    @MatchStep
    private MatchContext complete(MatchContext matchContext) {
        matchContext.setStatus(MatchStatus.PROCESSING);

        List<InternalOutputRecord> internalOutputRecords = distributeResults(matchContext.getInternalResults(),
                matchContext.getResultsBySource());
        matchContext.setInternalResults(internalOutputRecords);
        matchContext = mergeResults(matchContext);

        matchContext = appendMetadata(matchContext);

        matchContext.getOutput().setFinishedAt(new Date());
        Long receiveTime = matchContext.getOutput().getReceivedAt().getTime();
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - receiveTime);

        matchContext.setStatus(MatchStatus.PROCESSED);
        return matchContext;
    }

    @MatchStep
    private void generateOutputMetrics(MatchContext matchContext) {
        MatchInput input = matchContext.getInput();

        RealTimeResponse response = new RealTimeResponse(matchContext);
        metricService.write(MetricDB.LDC_Match, response);

        List<MatchedAccount> accountMeasurements = new ArrayList<>();
        List<MatchedColumn> columnMeasurements = new ArrayList<>();
        List<InternalOutputRecord> recordList = matchContext.getInternalResults();
        List<String> outputFields = matchContext.getOutput().getOutputFields();
        for (InternalOutputRecord record : recordList) {
            if (record.getFailed()) { continue; }
            MatchKeyDimension keyDimension =
                    new MatchKeyDimension(record.getParsedDomain(), record.getParsedNameLocation());
            MatchedAccount measurement = new MatchedAccount(input, keyDimension, matchContext.getMatchEngine(),
                    record.isMatched());
            accountMeasurements.add(measurement);

            for (int i = 0; i < outputFields.size(); i++) {
                String outputField = outputFields.get(i);
                Boolean matched = record.getColumnMatched().get(i);
                MatchedColumn matchedColumn = new MatchedColumn(matched, outputField, input, keyDimension,
                        matchContext.getMatchEngine());
                columnMeasurements.add(matchedColumn);
            }

        }

        metricService.write(MetricDB.LDC_Match, accountMeasurements);
        metricService.write(MetricDB.LDC_Match, columnMeasurements);
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
            if (record.getFailed()) { continue; }
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

                            if (countryInRow != null
                                    && !parsedCountry.equalsIgnoreCase((String) countryInRow)) {
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
            if (internalRecord.getFailed()) {
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

                if (IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)
                        && StringUtils.isNotEmpty(internalRecord.getParsedDomain())
                        && publicDomainService.isPublicDomain(internalRecord.getParsedDomain())) {
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

                if (ColumnSelection.Predefined.Model.equals(matchContext.getInput().getPredefinedSelection())) {
                    columnMatchCount[i] += (value == null ? 0 : 1);
                    internalRecord.getColumnMatched().add(value != null);
                } else {
                    columnMatchCount[i] += (matched ? 1 : 0);
                    internalRecord.getColumnMatched().add(matched);
                }

                matchedAnyColumn = matchedAnyColumn || matched;
            }


            // make IsMatched are not null
            if (matchedAnyColumn) {
                for (int i = 0; i < outputFields.size(); i++) {
                    String field = outputFields.get(i);
                    Object value = output.get(i);
                    if (field.toLowerCase().contains("ismatched") && value == null) {
                        output.set(i, false);
                    }
                    if (IS_PUBLIC_DOMAIN.equalsIgnoreCase(field)) {
                        output.set(i, publicDomainService.isPublicDomain(internalRecord.getParsedDomain()));
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

    private MatchContext appendMetadata(MatchContext matchContext) {
        if (ColumnSelection.Predefined.Model.equals(matchContext.getInput().getPredefinedSelection())) {
            List<ColumnMetadata> allFields = columnMetadataService
                    .fromPredefinedSelection(ColumnSelection.Predefined.Model);
            matchContext.getOutput().setMetadata(allFields);
        }
        return matchContext;
    }

    private boolean isDomainSource(String sourceName) {
        return true;
    }

    private boolean isCachedSource(String sourceName) {
        return MODEL.equals(sourceName);
    }

    private String getDomainField(String sourceName) {
        return "Domain";
    }

    private String getNameField(String sourceName) { return "Name"; }

    private String getCityField(String sourceName) { return "City"; }

    private String getStateField(String sourceName) { return "State"; }

    private String getCountryField(String sourceName) { return "Country"; }

    private MatchCallable getMatchCallable(String sourceName, List<String> targetColumns, MatchContext matchContext) {
        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
        if (MODEL.equals(sourceName)) {
            CachedMatchCallable callable =
                    new CachedMatchCallable("Domain", "Name", "Country", "State", "City");
            callable.setSourceName(sourceName);
            callable.setTargetColumns(targetColumns);
            callable.setJdbcTemplate(jdbcTemplate);
            callable.setDomainSet(matchContext.getDomains());
            callable.setNameLocationSet(matchContext.getNameLocations());
            callable.setRootOperationUID(matchContext.getOutput().getRootOperationUID());
            return callable;
        } else {
            throw new UnsupportedOperationException(
                    "Only support match against predefined selection " + ColumnSelection.Predefined.Model + " now.");
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
            List<Map<String, Object>> results = jdbcTemplate
                    .queryForList(constructSqlQuery(targetColumns, getSourceTableName(), domainSet, nameLocationSet));
            log.info("Retrieved " + results.size() + " results from SQL table " + getSourceTableName() + ". Duration="
                    + (System.currentTimeMillis() - beforeQuerying) + " RootOperationUID=" + rootOperationUID);
            submitSqlMetric(getSourceTableName(), jdbcTemplate, results.size(), targetColumns.size(),
                    System.currentTimeMillis() - beforeQuerying);
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

            for (String columnName: columns) {
                if (columnsInTable.contains(columnName.toLowerCase())) {
                    columnsToQuery.add(columnName);
                } else {
                    log.warn("Cannot find column " + columnName + " from table " + tableName);
                }
            }

            String sql = "SELECT "
                    + "[" + nameField + "], "
                    + "[" + countryField + "], "
                    + "[" + stateField + "], "
                    + ( cityField != null ? "[" + cityField + "], " : "" )
                    + (columnsToQuery.isEmpty() ? "" : "[" + StringUtils.join(columnsToQuery, "], [") + "], ")
                    + "[" + domainField + "] \n"
                    + "FROM [" + tableName + "] WITH(NOLOCK) \n"
                    + "WHERE [" + domainField + "] IN ('" + StringUtils.join(domains, "', '") + "')\n";

            for (NameLocation nameLocation : nameLocations) {
                if (StringUtils.isNotEmpty(nameLocation.getName())) {
                    sql += " OR ( ";
                    sql += String.format("[%s] = '%s'", nameField, nameLocation.getName().replace("'", "''"));
                    if (StringUtils.isNotEmpty(nameLocation.getCountry())) {
                        sql += String.format(" AND [%s] = '%s'", countryField, nameLocation.getCountry().replace("'", "''"));
                    }
                    if (StringUtils.isNotEmpty(nameLocation.getState())) {
                        sql += String.format(" AND [%s] = '%s'", stateField, nameLocation.getState().replace("'", "''"));
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

    private class DomainBasedMatchCallable extends MatchCallable implements Callable<List<Map<String, Object>>> {

        private Log log = LogFactory.getLog(DomainBasedMatchCallable.class);

        private Set<String> domainSet;

        public void setDomainSet(Set<String> domainSet) {
            this.domainSet = domainSet;
        }

        @Override
        public List<Map<String, Object>> call() {
            Long beforeQuerying = System.currentTimeMillis();
            List<Map<String, Object>> results = jdbcTemplate
                    .queryForList(constructSqlQuery(targetColumns, getSourceTableName(), domainSet));
            log.info("Retrieved " + results.size() + " results from SQL table " + getSourceTableName() + ". Duration="
                    + (System.currentTimeMillis() - beforeQuerying) + " RootOperationUID=" + rootOperationUID);
            submitSqlMetric(getSourceTableName(), jdbcTemplate, results.size(), targetColumns.size(),
                    System.currentTimeMillis() - beforeQuerying);
            return results;
        }

        private String constructSqlQuery(List<String> columns, String tableName, Collection<String> domains) {
            return "SELECT [" + StringUtils.join(columns, "], [") + "Domain] \n" + "FROM [" + tableName
                    + "] WITH(NOLOCK) \n" + "WHERE [Domain] IN ('" + StringUtils.join(domains, "', '") + "')";
        }
    }

    private void submitSqlMetric(String tableName, JdbcTemplate jdbcTemplate, Integer rows, Integer cols,
                                 Long timeElapsed) {
        try {
            SqlQueryMetric metric = new SqlQueryMetric();
            metric.setTableName(tableName);
            metric.setCols(cols);
            metric.setRows(rows);
            metric.setTimeElapsed(Integer.valueOf(String.valueOf(timeElapsed)));

            Connection connection = jdbcTemplate.getDataSource().getConnection();
            String productName = connection.getMetaData().getDatabaseProductName();
            metric.setServerType(productName);

            Pattern pattern = Pattern.compile("//(.*?)[:;/$]");
            Matcher matcher = pattern.matcher(connection.getMetaData().getURL());
            if (matcher.find()) {
                String token = matcher.group(0);
                String host = token.substring(2, token.length() - 1);
                metric.setHostName(host);
            }

            FetchDataFromSql fetchDataFromSql = new FetchDataFromSql(metric);
            metricService.write(MetricDB.LDC_Match, fetchDataFromSql);

        } catch (Exception e) {
            log.warn("Failed to store sql metric.", e);
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
            if (MODEL.equals(sourceName)) {
                return CACHE_TABLE;
            } else {
                return null;
            }
        }

    }

    private void buildSourceColumnMapCache() {
        tableColumnsCache = CacheBuilder.newBuilder().concurrencyLevel(4).weakKeys()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Set<String>>() {
                    public Set<String> load(String key) {
                        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
                        List<String> columnsByQuery = jdbcTemplate.queryForList("SELECT COLUMN_NAME "
                                + "FROM INFORMATION_SCHEMA.COLUMNS\n"
                                + "WHERE TABLE_NAME = '" + key
                                + "' AND TABLE_SCHEMA='dbo'", String.class);
                        Set<String> columnsInSql = new HashSet<>();
                        for (String columnName: columnsByQuery) {
                            columnsInSql.add(columnName.toLowerCase());
                        }
                        return columnsInSql;
                    }
                });
    }

}
