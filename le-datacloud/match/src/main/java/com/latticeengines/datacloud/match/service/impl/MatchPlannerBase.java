package com.latticeengines.datacloud.match.service.impl;

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
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.datacloud.match.service.CountryCodeService;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.newrelic.api.agent.Trace;

public abstract class MatchPlannerBase implements MatchPlanner {

    private static Log log = LogFactory.getLog(MatchPlannerBase.class);

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private BeanDispatcherImpl beanDispatcher;

    @Autowired
    private CountryCodeService countryCodeService;

    void assignAndValidateColumnSelectionVersion(MatchInput input) {
        if (input.getPredefinedSelection() != null) {
            ColumnSelectionService columnSelectionService = beanDispatcher
                    .getColumnSelectionService(input.getDataCloudVersion());
            input.setPredefinedVersion(validateOrAssignPredefinedVersion(columnSelectionService,
                    input.getPredefinedSelection(), input.getPredefinedVersion()));
        }
    }

    @Trace
    public ColumnSelection parseColumnSelection(MatchInput input) {
        ColumnSelectionService columnSelectionService = beanDispatcher
                .getColumnSelectionService(input.getDataCloudVersion());
        if (input.getUnionSelection() != null) {
            return combineSelections(columnSelectionService, input.getUnionSelection());
        } else if (input.getPredefinedSelection() != null) {
            return columnSelectionService.parsePredefinedColumnSelection(input.getPredefinedSelection());
        } else {
            return input.getCustomSelection();
        }
    }

    @MatchStep(threshold = 100L)
    public ColumnSelection combineSelections(ColumnSelectionService columnSelectionService,
            UnionSelection unionSelection) {
        List<ColumnSelection> selections = new ArrayList<>();
        for (Map.Entry<Predefined, String> entry : unionSelection.getPredefinedSelections().entrySet()) {
            Predefined predefined = entry.getKey();
            validateOrAssignPredefinedVersion(columnSelectionService, predefined, entry.getValue());
            selections.add(columnSelectionService.parsePredefinedColumnSelection(predefined));
        }
        if (unionSelection.getCustomSelection() != null && !unionSelection.getCustomSelection().isEmpty()) {
            selections.add(unionSelection.getCustomSelection());
        }
        return ColumnSelection.combine(selections);
    }

    protected String validateOrAssignPredefinedVersion(ColumnSelectionService columnSelectionService,
            Predefined predefined, String version) {
        if (StringUtils.isEmpty(version)) {
            version = columnSelectionService.getCurrentVersion(predefined);
            log.debug("Assign version " + version + " to column selection " + predefined);
        }
        return version;
    }

    // this is a dispatcher method
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

        DbHelper dbHelper = beanDispatcher.getDbHelper(input.getDataCloudVersion());
        dbHelper.populateMatchHints(context);

        return context;
    }

    @MatchStep(threshold = 100L)
    MatchContext sketchExecutionPlan(MatchContext matchContext, boolean skipExecutionPlanning) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        dbHelper.sketchExecutionPlan(matchContext, skipExecutionPlanning);
        return matchContext;
    }

    MatchOutput initializeMatchOutput(MatchInput input, ColumnSelection columnSelection,
            List<ColumnMetadata> metadatas) {
        MatchOutput output = new MatchOutput(input.getRootOperationUid());
        output.setReceivedAt(new Date());
        output.setInputFields(input.getFields());
        output.setKeyMap(input.getKeyMap());
        output.setSubmittedBy(input.getTenant());
        if (metadatas != null && !metadatas.isEmpty()) {
            output = appendMetadata(output, metadatas);
        } else {
            output = appendMetadata(output, columnSelection, input.getDataCloudVersion());
        }
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

        parseRecordForDomain(inputRecord, keyPositionMap, domainSet, excludePublicDomains, record);

        if (excludePublicDomains && record.isPublicDomain()) {
            return null;
        }

        parseRecordForNameLocation(inputRecord, keyPositionMap, nameLocationSet, record);
        parseRecordForDuns(inputRecord, keyPositionMap, record);
        parseRecordForLatticeAccountId(inputRecord, keyPositionMap, record);

        return record;
    }

    private void parseRecordForDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            Set<String> domainSet, boolean excludePublicDomains, InternalOutputRecord record) {
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
                    record.setPublicDomain(true);
                    if (excludePublicDomains) {
                        log.warn("A record with public domain is excluded from input.");
                        return;
                    }
                } else if (StringUtils.isNotEmpty(cleanDomain)) {
                    // update domain set
                    record.setPublicDomain(false);
                    domainSet.add(cleanDomain);
                }
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessage("Error when cleanup domain field: " + e.getMessage());
            }
        }
    }

    private void parseRecordForNameLocation(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            Set<NameLocation> nameLocationSet, InternalOutputRecord record) {
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
                    String countryCode = countryCodeService.getCountryCode(cleanCountry);

                    String originalState = null;
                    for (Integer statePos : statePosList) {
                        originalState = (String) inputRecord.get(statePos);
                    }
                    String cleanState = LocationUtils.getStandardState(cleanCountry, originalState);

                    NameLocation nameLocation = new NameLocation();
                    nameLocation.setName(originalName);
                    nameLocation.setState(cleanState);
                    nameLocation.setCountry(cleanCountry);
                    nameLocation.setCountryCode(countryCode);

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
    }

    private void parseRecordForDuns(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            InternalOutputRecord record) {
        if (keyPositionMap.containsKey(MatchKey.DUNS)) {
            List<Integer> dunsPosList = keyPositionMap.get(MatchKey.DUNS);
            try {
                String cleanDuns = null;
                for (Integer dunsPos : dunsPosList) {
                    String originalDuns = String.valueOf(inputRecord.get(dunsPos));
                    if (StringUtils.isNotEmpty(originalDuns)) {
                        cleanDuns = originalDuns;
                        break;
                    }
                }
                record.setParsedDuns(cleanDuns);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessage("Error when cleanup duns field: " + e.getMessage());
            }
        }
    }

    private void parseRecordForLatticeAccountId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
                                    InternalOutputRecord record) {
        if (keyPositionMap.containsKey(MatchKey.LatticeAccountID)) {
            List<Integer> idPosList = keyPositionMap.get(MatchKey.LatticeAccountID);
            try {
                String cleanId = null;
                for (Integer idPos : idPosList) {
                    String originalId = String.valueOf(inputRecord.get(idPos));
                    if (StringUtils.isNotEmpty(originalId)) {
                        cleanId = originalId;
                        break;
                    }
                }
                record.setLatticeAccountId(cleanId);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessage("Error when cleanup lattice account id field: " + e.getMessage());
            }
        }
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
    @Trace
    private MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection selection, String dataCloudVersion) {
        ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
        List<ColumnMetadata> metadata = columnMetadataService.fromSelection(selection, dataCloudVersion);
        matchOutput.setMetadata(metadata);
        return matchOutput;
    }

    @MatchStep(threshold = 100L)
    @Trace
    private MatchOutput appendMetadata(MatchOutput matchOutput, List<ColumnMetadata> metadata) {
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
