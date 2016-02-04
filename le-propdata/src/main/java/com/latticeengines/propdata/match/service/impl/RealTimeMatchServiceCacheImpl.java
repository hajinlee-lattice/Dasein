package com.latticeengines.propdata.match.service.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.propdata.core.datasource.DataSourcePool;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ColumnSelectionService;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

@Component("realTimeMatchServiceCache")
public class RealTimeMatchServiceCacheImpl implements RealTimeMatchService {

    private static Log log = LogFactory.getLog(RealTimeMatchServiceCacheImpl.class);

    private static final String CACHE_TABLE = "DerivedColumnsCache";
    private static final String MODEL = ColumnSelection.Predefined.Model.getName();

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private ColumnSelectionService columnSelectionService;

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    public MatchOutput match(MatchInput input, boolean returnUnmatched) {
        Long startTime = System.currentTimeMillis();

        validateMatchInput(input);

        MatchContext matchContext = prepare(input);

        matchContext.setStatus(MatchStatus.FETCHING);
        matchContext.setSourceColumnsMap(columnSelectionService.getSourceColumnMap(ColumnSelection.Predefined.Model));
        List<String> targetColumns = matchContext.getSourceColumnsMap().get(MODEL);
        Long beforeQuerying = System.currentTimeMillis();
        JdbcTemplate jdbcTemplate = dataSourceService.getJdbcTemplateFromDbPool(DataSourcePool.SourceDB);

        try {
            log.info("Got a JdbcTemplate for " + jdbcTemplate.getDataSource().getConnection().getMetaData().getURL());
        } catch (SQLException e) {
            // ignore
        }

        List<Map<String, Object>> results = jdbcTemplate
                .queryForList(constructSqlQuery(targetColumns, matchContext.getDomains()));
        matchContext.setStatus(MatchStatus.FETCHED);
        log.info("Retrieved " + results.size() + " results from SQL Server. Duration="
                + (System.currentTimeMillis() - beforeQuerying));

        matchContext.setStatus(MatchStatus.PROCESSING);
        Long beforeProcessing = System.currentTimeMillis();
        matchContext = parseResult(matchContext, MODEL, results, returnUnmatched);

        List<ColumnMetadata> allFields = columnMetadataService
                .fromPredefinedSelection(ColumnSelection.Predefined.Model);
        List<String> outputFields = new ArrayList<>();
        Set<String> columnSet = new HashSet<>(targetColumns);
        for (ColumnMetadata field : allFields) {
            if (columnSet.contains(field.getColumnName())) {
                outputFields.add(field.getColumnName());
            }
        }
        matchContext.getOutput().setOutputFields(outputFields);
        matchContext.getOutput().setMetadata(allFields);

        matchContext.getOutput().setFinishedAt(new Date());
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - startTime);

        log.info("Processed " + results.size() + " results into MatchOutput. Duration="
                + (System.currentTimeMillis() - beforeProcessing));
        matchContext.setStatus(MatchStatus.PROCESSED);

        matchContext.setStatus(MatchStatus.FINISHED);
        return matchContext.getOutput();
    }

    private void validateMatchInput(MatchInput input) {
        Long startTime = System.currentTimeMillis();
        MatchInputValidator.validate(input, zkConfigurationService.maxRealTimeInput());
        log.info("Finished validating match input for " + input.getData().size() + " rows. Duration="
                + (System.currentTimeMillis() - startTime));
    }

    private MatchContext prepare(MatchInput input) {
        Long startTime = System.currentTimeMillis();

        MatchContext context = MatchPlanner.plan(input);

        log.info("Finished preparing match context for " + input.getData().size() + " rows. Duration="
                + (System.currentTimeMillis() - startTime));
        return context;
    }

    private String constructSqlQuery(List<String> columns, Collection<String> domains) {
        return "SELECT [Domain], [" + StringUtils.join(columns, "], [") + "] \n" + "FROM [" + CACHE_TABLE + "] \n"
                + "WHERE [Domain] IN ('" + StringUtils.join(domains, "', '") + "')";
    }

    private MatchContext parseResult(MatchContext matchContext, String sourceName, List<Map<String, Object>> results,
            boolean returnUnmatched) {
        List<String> targetColumns = matchContext.getSourceColumnsMap().get(sourceName);
        Integer[] columnMatchCount = new Integer[targetColumns.size()];

        Map<String, List<Object>> domainMap = domainResults(results, targetColumns);

        List<InternalOutputRecord> recordsToMatch = matchContext.getInternalResults();
        List<OutputRecord> outputRecords = new ArrayList<>();
        int matched = 0;
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
                record.addErrorMessage("Could not find a match for domain [" + record.getMatchedDomain() + "] in table "
                        + CACHE_TABLE);
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

    Map<String, List<Object>> domainResults(List<Map<String, Object>> results, List<String> targetColumns) {
        Map<String, Integer> posMap = new HashMap<>();
        int pos = 0;
        for (String column : targetColumns) {
            posMap.put(column, pos);
            pos++;
        }

        Map<String, List<Object>> toReturn = new HashMap<>();
        for (Map<String, Object> record : results) {
            String domain = (String) record.get("Domain");
            if (!toReturn.containsKey(domain)) {
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

}
