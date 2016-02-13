package com.latticeengines.propdata.match.service.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyDimension;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.MatchedAccount;
import com.latticeengines.propdata.match.metric.MatchedColumn;
import com.latticeengines.propdata.match.metric.RealTimeResponse;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.MatchExecutor;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);
    private static final String CACHE_TABLE = "DerivedColumnsCache";
    private static final String MODEL = ColumnSelection.Predefined.Model.getName();

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private MetricService metricService;

    private ExecutorService executor = Executors.newFixedThreadPool(4);

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

        List<ColumnMetadata> allFields = columnMetadataService
                .fromPredefinedSelection(ColumnSelection.Predefined.Model);
        matchContext.getOutput().setMetadata(allFields);

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
        Map<MatchKey, String> keyMap = input.getKeyMap();
        List<String> inputFields = input.getFields();
        List<String> outputFields = matchContext.getOutput().getOutputFields();
        for (InternalOutputRecord record : recordList) {
            List<Object> inputData = record.getInput();
            MatchKeyDimension keyDimension = new MatchKeyDimension(keyMap, inputFields, inputData);
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
            if (isDomainSource(sourceName)) {
                String domainField = getDomainField(sourceName);
                for (InternalOutputRecord record : records) {
                    String parsedDomain = record.getParsedDomain();
                    if (StringUtils.isEmpty(parsedDomain)) {
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

        for (InternalOutputRecord record : records) {
            record.setColumnMatched(new ArrayList<Boolean>());
            List<Object> output = new ArrayList<>();
            Map<String, Map<String, Object>> results = record.getResultsInSource();
            boolean matchedAnyColumn = false;
            for (int i = 0; i < outputFields.size(); i++) {
                String field = outputFields.get(i);
                Object value = null;
                boolean matched = false;
                if (columnPriorityMap.containsKey(field)) {
                    for (String targetSource : columnPriorityMap.get(field)) {
                        if (results.containsKey(targetSource)) {
                            matched = true;
                            Object objInResult = results.get(targetSource).get(field);
                            value = (objInResult == null ? value : objInResult);
                        }
                    }
                }

                output.add(value);

                if (matchContext.getInput().getPredefinedSelection().equals(ColumnSelection.Predefined.Model)) {
                    columnMatchCount[i] += (value == null ? 0 : 1);
                    record.getColumnMatched().add(value != null);
                } else {
                    columnMatchCount[i] += (matched ? 1 : 0);
                    record.getColumnMatched().add(matched);
                }

                matchedAnyColumn = matchedAnyColumn || matched;
            }

            record.setOutput(output);

            if (matchedAnyColumn) {
                totalMatched++;
                record.setMatched(true);
            }

            if (returnUnmatched || matchedAnyColumn) {
                record.setResultsInSource(null);
                outputRecords.add(record);
            }
        }

        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(totalMatched);
        matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));

        return matchContext;
    }

    private boolean isDomainSource(String sourceName) {
        return true;
    }

    private String getDomainField(String sourceName) {
        return "Domain";
    }

    private MatchCallable getMatchCallable(String sourceName, List<String> targetColumns, MatchContext matchContext) {
        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);
        if (MODEL.equals(sourceName)) {
            DomainBasedMatchCallable callable = new DomainBasedMatchCallable();
            callable.setSourceName(sourceName);
            callable.setTargetColumns(targetColumns);
            callable.setJdbcTemplate(jdbcTemplate);
            callable.setDomainSet(matchContext.getDomains());
            callable.setRootOperationUID(matchContext.getOutput().getRootOperationUID());
            return callable;
        } else {
            throw new UnsupportedOperationException(
                    "Only support match against predefined selection " + ColumnSelection.Predefined.Model + " now.");
        }
    }

    private static class DomainBasedMatchCallable extends MatchCallable implements Callable<List<Map<String, Object>>> {

        private static Log log = LogFactory.getLog(DomainBasedMatchCallable.class);

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
            submitMetric(results.size(), targetColumns.size());
            return results;
        }

        private void submitMetric(Integer rows, Integer cols) {
            try {
                Connection connection = jdbcTemplate.getDataSource().getConnection();
                String productName = connection.getMetaData().getDatabaseProductName();
                System.out.println(productName);
            } catch (Exception e) {
                log.warn("Failed to store sql metric.", e);
            }
        }

        private static String constructSqlQuery(List<String> columns, String tableName, Collection<String> domains) {
            return "SELECT [Domain], [" + StringUtils.join(columns, "], [") + "] \n" + "FROM [" + tableName + "] \n"
                    + "WHERE [Domain] IN ('" + StringUtils.join(domains, "', '") + "')";
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

}
