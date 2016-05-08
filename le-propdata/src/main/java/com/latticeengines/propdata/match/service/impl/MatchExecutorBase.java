package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.DisposableEmailService;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.PublicDomainService;

public abstract class MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);

    @Autowired
    private ColumnMetadataService columnMetadataService;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private DisposableEmailService disposableEmailService;

    @Autowired
    protected MetricService metricService;

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
    public MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection.Predefined selection) {
        if (ColumnSelection.Predefined.Model.equals(selection)
                || ColumnSelection.Predefined.DerivedColumns.equals(selection)) {
            List<ColumnMetadata> metadata = columnMetadataService.fromPredefinedSelection(selection);
            matchOutput.setMetadata(metadata);
            return matchOutput;
        }
        throw new RuntimeException("Cannot find the requested metadata.");
    }

     protected void generateAccountMetric(MatchContext matchContext) {
         // no need to push this metric to influxdb now.
         // will figure out another way to dump match history.
         /*
         try {
            MatchInput input = matchContext.getInput();
            List<MatchedAccount> accountMeasurements = new ArrayList<>();
            List<InternalOutputRecord> recordList = matchContext.getInternalResults();
            for (InternalOutputRecord record : recordList) {
                if (record.isFailed()) {
                    continue;
                }
                MatchKeyDimension keyDimension =
                        new MatchKeyDimension(record.getParsedDomain(), record.getParsedNameLocation());
                MatchedAccount measurement = new MatchedAccount(input, keyDimension, matchContext.getMatchEngine(),
                        record.isMatched());
                accountMeasurements.add(measurement);
            }

            // influxdb cannot host history of this, so it is not useful to dump this to infuxdb.
            // metricService.write(MetricDB.LDC_Match, accountMeasurements);
        } catch (Exception e) {
            log.warn("Failed to extract account based metric.", e);
        }
        */
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

}
