package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;
import com.latticeengines.domain.exposed.propdata.match.NameLocation;
import com.latticeengines.domain.exposed.propdata.match.UnionSelection;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.MatchRequest;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.service.ColumnSelectionService;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.latticeengines.propdata.match.service.PublicDomainService;

public abstract class MatchPlannerBase implements MatchPlanner {

    private static Log log = LogFactory.getLog(MatchPlannerBase.class);

    @Autowired
    protected ColumnSelectionService columnSelectionService;

    @Autowired
    protected ColumnMetadataService columnMetadataService;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private MetricService metricService;

    void assignAndValidateColumnSelectionVersion(MatchInput input) {
        if (input.getPredefinedSelection() != null) {
            input.setPredefinedVersion(
                    validateOrAssignPredefinedVersion(input.getPredefinedSelection(), input.getPredefinedVersion()));
        }
    }

    ColumnSelection parseColumnSelection(MatchInput input) {
        if (input.getUnionSelection() != null) {
            return combineSelections(input.getUnionSelection());
        } else if (input.getPredefinedSelection() != null) {
            return columnSelectionService.parsePredefined(input.getPredefinedSelection());
        } else {
            return input.getCustomSelection();
        }
    }

    @MatchStep(threshold = 100L)
    private ColumnSelection combineSelections(UnionSelection unionSelection) {
        List<ColumnSelection> selections = new ArrayList<>();
        for (Map.Entry<ColumnSelection.Predefined, String> entry : unionSelection.getPredefinedSelections()
                .entrySet()) {
            ColumnSelection.Predefined predefined = entry.getKey();
            validateOrAssignPredefinedVersion(predefined, entry.getValue());
            selections.add(columnSelectionService.parsePredefined(predefined));
        }
        if (unionSelection.getCustomSelection() != null && !unionSelection.getCustomSelection().isEmpty()) {
            selections.add(unionSelection.getCustomSelection());
        }
        return ColumnSelection.combine(selections);
    }

    private String validateOrAssignPredefinedVersion(ColumnSelection.Predefined predefined, String version) {
        if (StringUtils.isEmpty(version)) {
            version = columnSelectionService.getCurrentVersion(predefined);
            log.debug("Assign version " + version + " to column selection " + predefined);
        }
        if (!columnSelectionService.isValidVersion(predefined, version)) {
            throw new IllegalArgumentException(
                    "The specified version " + version + " is invalid for the selection " + predefined);
        }
        return version;
    }

    MatchContext scanInputData(MatchInput input, MatchContext context) {
        Map<MatchKey, List<Integer>> keyPositionMap = getKeyPositionMap(input);

        List<InternalOutputRecord> records = new ArrayList<>();
        Set<String> domainSet = new HashSet<>();
        Set<NameLocation> nameLocationSet = new HashSet<>();

        for (int i = 0; i < input.getData().size(); i++) {
            InternalOutputRecord record = scanInputRecordAndUpdateKeySets(input.getData().get(i), i,
                    input.getFields().size(), keyPositionMap, domainSet, nameLocationSet,
                    input.getExcludePublicDomains());
            if (record != null) {
                record.setColumnMatched(new ArrayList<Boolean>());
                records.add(record);
            }
        }

        context.setInternalResults(records);
        context.setDomains(domainSet);
        context.setNameLocations(nameLocationSet);
        return context;
    }

    @MatchStep(threshold = 100L)
    MatchContext sketchExecutionPlan(MatchContext matchContext) {
        ColumnSelection columnSelection = matchContext.getColumnSelection();
        matchContext.setPartitionColumnsMap(columnSelectionService.getPartitionColumnMap(columnSelection));
        return matchContext;
    }

    @Override
    public void generateInputMetric(MatchInput input) {
        try {
            ColumnSelection columnSelection = parseColumnSelection(input);
            Integer selectedCols = columnSelection.getColumns().size();
            MatchRequest request = new MatchRequest(input, selectedCols);
            metricService.write(MetricDB.LDC_Match, request);
        } catch (Exception e) {
            log.warn("Failed to extract input metric.", e);
        }
    }

    MatchOutput initializeMatchOutput(MatchInput input) {
        MatchOutput output = new MatchOutput(input.getUuid());
        output.setReceivedAt(new Date());
        output.setInputFields(input.getFields());
        output.setKeyMap(input.getKeyMap());
        output.setSubmittedBy(input.getTenant());
        output = appendMetadata(output, parseColumnSelection(input));
        output = parseOutputFields(output);
        MatchStatistics statistics = initializeStatistics(input);
        output.setStatistics(statistics);
        return output;
    }

    private static MatchStatistics initializeStatistics(MatchInput input) {
        MatchStatistics statistics = new MatchStatistics();
        statistics.setRowsRequested(input.getNumRows());
        return statistics;
    }

    private InternalOutputRecord scanInputRecordAndUpdateKeySets(List<Object> inputRecord, int rowNum,
            int numInputFields, Map<MatchKey, List<Integer>> keyPositionMap, Set<String> domainSet,
            Set<NameLocation> nameLocationSet, boolean excludePublicDomains) {
        InternalOutputRecord record = new InternalOutputRecord();
        record.setRowNumber(rowNum);
        record.setMatched(false);
        record.setInput(inputRecord);

        if (inputRecord.size() != numInputFields) {
            record.setFailed(true);
            record.addErrorMessage("The number of objects in this row [" + inputRecord.size()
                    + "] does not match the number of fields claimed [" + numInputFields + "]");
            return record;
        }

        if (keyPositionMap.containsKey(MatchKey.Domain)) {
            List<Integer> domainPosList = keyPositionMap.get(MatchKey.Domain);
            try {
                String cleanDomain = null;
                for (Integer domainPos : domainPosList) {
                    String originalDomain = (String) inputRecord.get(domainPos);
                    cleanDomain = DomainUtils.parseDomain(originalDomain);
                    if (StringUtils.isNotEmpty(cleanDomain)) {
                        break;
                    }
                }
                record.setParsedDomain(cleanDomain);
                if (publicDomainService.isPublicDomain(cleanDomain)) {
                    record.addErrorMessage("Parsed to a public domain: " + cleanDomain);
                    if (excludePublicDomains) {
                        log.warn("A record with publid domain is excluded from input.");
                        return null;
                    }
                } else if (StringUtils.isNotEmpty(cleanDomain)) {
                    // update domain set
                    domainSet.add(cleanDomain);
                }
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessage("Error when cleanup domain field: " + e.getMessage());
            }
        }

        if (keyPositionMap.containsKey(MatchKey.Name) && keyPositionMap.containsKey(MatchKey.State)
                && keyPositionMap.containsKey(MatchKey.Country)) {
            List<Integer> namePosList = keyPositionMap.get(MatchKey.Name);
            List<Integer> statePosList = keyPositionMap.get(MatchKey.State);
            List<Integer> countryPosList = keyPositionMap.get(MatchKey.Country);

            try {
                String originalName = null;
                for (Integer namePos : namePosList) {
                    originalName = (String) inputRecord.get(namePos);
                }
                if (StringUtils.isNotEmpty(originalName)) {
                    String originalCountry = null;
                    for (Integer countryPos : countryPosList) {
                        originalCountry = (String) inputRecord.get(countryPos);
                    }
                    if (StringUtils.isEmpty(originalCountry)) {
                        originalCountry = LocationUtils.USA;
                    }
                    String cleanCountry = LocationUtils.getStandardCountry(originalCountry);

                    String originalState = null;
                    for (Integer statePos : statePosList) {
                        originalState = (String) inputRecord.get(statePos);
                    }
                    String cleanState = LocationUtils.getStandardState(cleanCountry, originalState);

                    NameLocation nameLocation = new NameLocation();
                    nameLocation.setName(originalName);
                    nameLocation.setState(cleanState);
                    nameLocation.setCountry(cleanCountry);

                    if (keyPositionMap.containsKey(MatchKey.City)) {
                        String originalCity = null;
                        for (Integer cityPos : keyPositionMap.get(MatchKey.City)) {
                            originalCity = (String) inputRecord.get(cityPos);
                        }
                        nameLocation.setCity(originalCity);
                    }

                    record.setParsedNameLocation(nameLocation);
                    nameLocationSet.add(nameLocation);
                }
            } catch (Exception e) {
                log.error(ExceptionUtils.getFullStackTrace(e));
                record.setFailed(true);
                record.addErrorMessage("Error when cleanup name and location fields: " + e.getMessage());
            }
        }

        return record;
    }

    private static Map<MatchKey, List<Integer>> getKeyPositionMap(MatchInput input) {
        Map<String, Integer> fieldPos = new HashMap<>();
        for (int pos = 0; pos < input.getFields().size(); pos++) {
            fieldPos.put(input.getFields().get(pos).toLowerCase(), pos);
        }

        Map<MatchKey, List<Integer>> posMap = new HashMap<>();
        for (MatchKey key : input.getKeyMap().keySet()) {
            List<Integer> posList = new ArrayList<>();
            for (String field : input.getKeyMap().get(key)) {
                posList.add(fieldPos.get(field.toLowerCase()));
            }
            posMap.put(key, posList);
        }

        return posMap;
    }

    @MatchStep(threshold = 100L)
    private MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection selection) {
        List<ColumnMetadata> metadata = columnMetadataService.fromSelection(selection);
        matchOutput.setMetadata(metadata);
        return matchOutput;
    }

    private MatchOutput parseOutputFields(MatchOutput matchOutput) {
        List<ColumnMetadata> metadata = matchOutput.getMetadata();
        List<String> fields = new ArrayList<>();
        for (ColumnMetadata column : metadata) {
            fields.add(column.getColumnName());
        }
        matchOutput.setOutputFields(fields);
        return matchOutput;
    }

}
