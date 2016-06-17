package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ColumnSelectionService;
import com.latticeengines.propdata.match.service.DisposableEmailService;
import com.latticeengines.propdata.match.service.ExternalColumnService;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.PublicDomainService;

public abstract class MatchExecutorBase implements MatchExecutor {

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private ColumnSelectionService columnSelectionService;

    @Autowired
    private ExternalColumnService externalColumnService;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private DisposableEmailService disposableEmailService;

    @Autowired
    protected MetricService metricService;

    @MatchStep
    MatchContext complete(MatchContext matchContext) {
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
            distributeCachedSourceResults(records, sourceName, result.getValue());
        }
        return records;
    }

    private void distributeCachedSourceResults(List<InternalOutputRecord> records, String sourceName,
            List<Map<String, Object>> rows) {
        for (InternalOutputRecord record : records) {
            if (record.isFailed()) {
                continue;
            }
            // try using domain first
            boolean matched = false;
            String parsedDomain = record.getParsedDomain();
            if (StringUtils.isNotEmpty(parsedDomain)) {
                for (Map<String, Object> row : rows) {
                    if (row.containsKey(MatchConstants.DOMAIN_FIELD) && parsedDomain.equals(row.get(MatchConstants.DOMAIN_FIELD))) {
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
        List<String> columnNames = columnSelectionService.getMatchedColumns(matchContext.getColumnSelection());
        List<ColumnSelection.Column> columns = matchContext.getColumnSelection().getColumns();
        boolean returnUnmatched = matchContext.isReturnUnmatched();

        List<OutputRecord> outputRecords = new ArrayList<>();
        Integer[] columnMatchCount = new Integer[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
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
            for (int i = 0; i < columns.size(); i++) {
                ColumnSelection.Column column = columns.get(i);
                ExternalColumn externalColumn = externalColumnService.getExternalColumn(column.getExternalColumnId());
                String field = (externalColumn != null) ? externalColumn.getDefaultColumnName()
                        : column.getColumnName();

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
                } else if (externalColumn != null && StringUtils.isNotEmpty(externalColumn.getTablePartition())) {
                    String tableName = externalColumn.getTablePartition();
                    if (results.containsKey(tableName)) {
                        matched = true;
                        Object objInResult = results.get(tableName).get(field);
                        value = (objInResult == null ? value : objInResult);
                    }
                } else {
                    for (Map<String, Object> resultSet : results.values()) {
                        if (resultSet.containsKey(field)) {
                            matched = true;
                            Object objInResult = resultSet.get(field);
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
            for (int i = 0; i < columnNames.size(); i++) {
                String field = columnNames.get(i);
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

            internalRecord.setResultsInSource(null);
            OutputRecord outputRecord = new OutputRecord();
            if (returnUnmatched || matchedAnyColumn) {
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

        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(totalMatched);
        matchContext.getOutput().getStatistics().setColumnMatchCount(Arrays.asList(columnMatchCount));

        return matchContext;
    }

    @Override
    public MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection selection) {
        List<ColumnMetadata> metadata = columnMetadataService.fromSelection(selection);
        matchOutput.setMetadata(metadata);
        return matchOutput;
    }

}
