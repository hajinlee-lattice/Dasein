package com.latticeengines.propdata.match.service.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;
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
        MatchContext matchContext = prepare(input, returnUnmatched);

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
        List<ColumnMetadata> filtered = new ArrayList<>();
        Set<String> columnSet = new HashSet<>(targetColumns);
        for (ColumnMetadata field : allFields) {
            if (columnSet.contains(field.getColumnName())) {
                filtered.add(field);
            }
        }
        matchContext.getOutput().setMetadata(filtered);

        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.setTime(new Date());
        matchContext.getOutput().getStatistics().setResultGeneratedAt(calendar.getTime());
        matchContext.getOutput().getStatistics().setTimeElapsedInMsec(System.currentTimeMillis() - startTime);

        log.info("Processed " + results.size() + " results into MatchOutput. Duration="
                + (System.currentTimeMillis() - beforeProcessing));
        matchContext.setStatus(MatchStatus.PROCESSED);

        matchContext.setStatus(MatchStatus.FINISHED);
        return matchContext.getOutput();
    }

    @VisibleForTesting
    void validateMatchInput(MatchInput input) {
        Long startTime = System.currentTimeMillis();

        if (input.getTenant() == null) {
            throw new IllegalArgumentException("Must provide tenant to run a match.");
        }

        if (input.getMatchEngine() == null) {
            throw new IllegalArgumentException("Must specify match engine.");
        }

        if (input.getFields() == null || input.getFields().isEmpty()) {
            throw new IllegalArgumentException("Empty list of fields.");
        }

        if (input.getKeyMap() == null || input.getKeyMap().keySet().isEmpty()) {
            log.info("Did not find KeyMap in the input. Try to resolve the map from field list.");
            input.setKeyMap(MatchKeyUtils.resolveKeyMap(input.getFields()));
        }

        for (String field : input.getKeyMap().values()) {
            if (!input.getFields().contains(field)) {
                throw new IllegalArgumentException("Cannot find target field " + field + " in claimed field list.");
            }
        }

        validateKeys(input.getKeyMap().keySet());

        if (MatchInput.MatchEngine.RealTime.equals(input.getMatchEngine())) {
            if (input.getData() == null || input.getData().isEmpty()) {
                throw new IllegalArgumentException("Empty input data.");
            }

            if (input.getData().size() > zkConfigurationService.maxRealTimeInput()) {
                throw new IllegalArgumentException(
                        "Too many input data, maximum rows = " + zkConfigurationService.maxRealTimeInput());
            }
        } else {
            throw new UnsupportedOperationException(
                    "Match engine " + MatchInput.MatchEngine.Bulk + " is not supported.");
        }

        log.info("Finished validating match input for " + input.getData().size() + " rows. Duration="
                + (System.currentTimeMillis() - startTime));
    }

    private void validateKeys(Set<MatchKey> keySet) {
        if (!keySet.contains(MatchKey.Domain) && !keySet.contains(MatchKey.Name)) {
            throw new IllegalArgumentException("Neither domain nor name is provided.");
        }

        if (!keySet.contains(MatchKey.Domain)) {
            throw new UnsupportedOperationException("Only domain based match is supported for now.");
        }
    }

    @VisibleForTesting
    MatchContext prepare(MatchInput input, boolean returnUnmatched) {
        Long startTime = System.currentTimeMillis();

        MatchContext context = new MatchContext();

        MatchOutput output = new MatchOutput();
        context.setStatus(MatchStatus.NEW);
        output.setInputFields(input.getFields());
        output.setKeyMap(input.getKeyMap());

        MatchStatistics statistics = new MatchStatistics();
        statistics.setRowsRequested(input.getData().size());
        output.setStatistics(statistics);

        List<OutputRecord> records = new ArrayList<>();
        Set<String> domainSet = new HashSet<>();

        boolean hasDomain = false;
        Map<MatchKey, Integer> posMap = new HashMap<>();
        Set<MatchKey> keySet = input.getKeyMap().keySet();
        for (int pos = 0; pos < input.getFields().size(); pos++) {
            String field = input.getFields().get(pos);
            for (MatchKey key : keySet) {
                if (field.equals(input.getKeyMap().get(key))) {
                    posMap.put(key, pos);
                }
                if (MatchKey.Domain.equals(key)) {
                    hasDomain = true;
                }
            }
        }

        int domainPos = hasDomain ? posMap.get(MatchKey.Domain) : -1;
        for (int i = 0; i < input.getData().size(); i++) {
            OutputRecord record = new OutputRecord();
            record.setRowNumber(i);
            List<Object> row = input.getData().get(i);
            record.setInput(row);
            record.setMatched(true);

            if (row.size() != input.getFields().size()) {
                record.setMatched(false);
                record.setErrorMessage("The number of objects in this row [" + row.size()
                        + "] does not match the number of fields claimed [" + input.getFields().size() + "]");
            } else if (hasDomain) {
                try {
                    String originalDomain = (String) row.get(domainPos);
                    String cleanDomain = DomainUtils.parseDomain(originalDomain);
                    record.setMatchedDomain(cleanDomain);
                    domainSet.add(cleanDomain);
                } catch (Exception e) {
                    record.setMatched(false);
                    record.setErrorMessage("Error when cleanup domain field: " + e.getMessage());
                }
            }

            if (record.isMatched() || returnUnmatched) {
                record.setMatched(false); // change back to unmatched before
                                          // matching
                records.add(record);
            }
        }

        output.setResult(records);
        context.setDomains(domainSet);
        context.setOutput(output);

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

        List<OutputRecord> recordsToMatch = matchContext.getOutput().getResult();
        List<OutputRecord> outputRecords = new ArrayList<>();
        int matched = 0;
        for (OutputRecord record : recordsToMatch) {
            if (StringUtils.isEmpty(record.getErrorMessage()) && domainMap.containsKey(record.getMatchedDomain())) {
                record.setMatched(true);
                List<Object> output = domainMap.get(record.getMatchedDomain());
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

                outputRecords.add(record);
            } else if (returnUnmatched) {
                if (StringUtils.isEmpty(record.getErrorMessage())) {
                    record.setErrorMessage("Could not find a match for domain [" + record.getMatchedDomain()
                            + "] in table " + CACHE_TABLE);
                }
                record.setMatchedDomain(null);
                outputRecords.add(record);
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
