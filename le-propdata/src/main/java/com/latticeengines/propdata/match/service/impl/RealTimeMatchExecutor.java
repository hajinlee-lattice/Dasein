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

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.match.annotation.MatchStep;
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

    private ExecutorService executor = Executors.newFixedThreadPool(4);

    @Override
    public MatchContext executeMatch(MatchContext matchContext) {
        matchContext = fetchData(matchContext);
        matchContext = mangleResults(matchContext);
        return matchContext;
    }

    @MatchStep
    private MatchContext fetchData(MatchContext context) {
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
    private MatchContext mangleResults(MatchContext matchContext) {
        matchContext.setStatus(MatchStatus.PROCESSING);

        List<Map<String, Object>> results = matchContext.getResultsBySource().get(MODEL);
        matchContext = parseResult(matchContext, MODEL, results);

        List<ColumnMetadata> allFields = columnMetadataService
                .fromPredefinedSelection(ColumnSelection.Predefined.Model);
        List<String> outputFields = new ArrayList<>();
        List<String> targetColumns = matchContext.getSourceColumnsMap().get(MODEL);
        Set<String> columnSet = new HashSet<>(targetColumns);
        for (ColumnMetadata field : allFields) {
            if (columnSet.contains(field.getColumnName())) {
                outputFields.add(field.getColumnName());
            }
        }
        matchContext.getOutput().setOutputFields(outputFields);
        matchContext.getOutput().setMetadata(allFields);

        matchContext.getOutput().setFinishedAt(new Date());
        Long receiveTime = matchContext.getOutput().getReceivedAt().getTime();
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - receiveTime);

        matchContext.setStatus(MatchStatus.PROCESSED);
        return matchContext;
    }

    private MatchContext parseResult(MatchContext matchContext, String sourceName, List<Map<String, Object>> results) {
        List<String> targetColumns = matchContext.getSourceColumnsMap().get(sourceName);
        Integer[] columnMatchCount = new Integer[targetColumns.size()];

        Map<String, List<Object>> domainMap = parseDomainBasedResults(results, targetColumns, "Domain");

        List<InternalOutputRecord> recordsToMatch = matchContext.getInternalResults();
        List<OutputRecord> outputRecords = new ArrayList<>();
        int matched = 0;
        boolean returnUnmatched = matchContext.isReturnUnmatched();
        for (InternalOutputRecord record : recordsToMatch) {
            if (domainMap.containsKey(record.getParsedDomain())) {
                record.setMatched(true);
                List<Object> output = domainMap.get(record.getParsedDomain());
                record.setOutput(output);

                matched++;
                for (int i = 0; i < output.size(); i++) {
                    if (columnMatchCount[i] == null) {
                        columnMatchCount[i] = 0;
                    }
                    if (output.get(i) != null) {
                        columnMatchCount[i]++;
                    }
                }
                record.setMatchedDomain(record.getParsedDomain());
                outputRecords.add(record);
            } else {
                record.addErrorMessage("Could not find a match for domain [" + record.getMatchedDomain()
                        + "] in source " + sourceName);
                record.setMatchedDomain(null);
                if (returnUnmatched) {
                    outputRecords.add(record);
                }
            }
        }

        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));
        matchContext.getOutput().getStatistics().setRowsMatched(matched);
        return matchContext;
    }

    private Map<String, List<Object>> parseDomainBasedResults(List<Map<String, Object>> results,
            List<String> targetColumns, String sourceDomainField) {
        Map<String, Integer> posMap = new HashMap<>();
        int pos = 0;
        for (String column : targetColumns) {
            posMap.put(column, pos);
            pos++;
        }

        Map<String, List<Object>> toReturn = new HashMap<>();
        for (Map<String, Object> record : results) {
            String domain = DomainUtils.parseDomain((String) record.get(sourceDomainField));
            if (StringUtils.isNotEmpty(domain) && !toReturn.containsKey(domain)) {
                Object[] data = new Object[targetColumns.size()];
                for (Map.Entry<String, Object> entry : record.entrySet()) {
                    if (posMap.containsKey(entry.getKey())) {
                        data[posMap.get(entry.getKey())] = entry.getValue();
                    }
                }
                toReturn.put(domain, Arrays.asList(data));
            }
        }

        return toReturn;
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
            return results;
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
