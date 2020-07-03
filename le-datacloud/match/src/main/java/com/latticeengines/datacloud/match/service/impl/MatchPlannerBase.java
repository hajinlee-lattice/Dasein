package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.util.Utf8;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.datacloud.match.util.DirectPlusUtils;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public abstract class MatchPlannerBase implements MatchPlanner {

    private static final Logger log = LoggerFactory.getLogger(MatchPlannerBase.class);

    @Inject
    private PublicDomainService publicDomainService;

    @Inject
    private BeanDispatcherImpl beanDispatcher;

    @Inject
    private NameLocationService nameLocationService;

    @Inject
    private ZkConfigurationService zkConfigurationService;

    @Inject
    private CDLLookupService cdlColumnSelectionService;

    @Inject
    private MatchDecisionGraphService matchDecisionGraphService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Value("${datacloud.match.default.decision.graph.account}")
    private String defaultAccountGraph;

    @Value("${datacloud.match.default.decision.graph.contact}")
    private String defaultContactGraph;

    @Value("${datacloud.match.default.decision.graph.prime}")
    private String defaultPrimeGraph;

    /**
     * Default DataCloud version is latest approved version with major version as
     * 2.0
     *
     * @param input
     */
    void setDataCloudVersion(MatchInput input) {
        if (StringUtils.isBlank(input.getDataCloudVersion())) {
            String dcVersion = versionEntityMgr.currentApprovedVersionAsString();
            log.warn("Found a match request without DataCloud version, force to use {}. MatchInput={}", dcVersion,
                    JsonUtils.serialize(input));
            input.setDataCloudVersion(dcVersion);
        }
    }

    void setDecisionGraph(MatchInput input) {
        String decisionGraph = input.getDecisionGraph();
        if (StringUtils.isEmpty(decisionGraph)) {
            if (BusinessEntity.PrimeAccount.name().equalsIgnoreCase(input.getTargetEntity())) {
                decisionGraph = defaultPrimeGraph;
            } else {
                decisionGraph = defaultGraph;
            }
            input.setDecisionGraph(decisionGraph);
            log.debug("Did not specify decision graph, use the default one: " + decisionGraph);
        }
    }

    public void setEntityDecisionGraph(MatchInput input) {
        // No need to handle cases that both targetEntity and decisionGraph are
        // empty or populated. These 2 cases are already handled in validator
        if (StringUtils.isBlank(input.getDecisionGraph())) {
            if (BusinessEntity.Account.name().equals(input.getTargetEntity())) {
                input.setDecisionGraph(defaultAccountGraph);
            } else if (BusinessEntity.Contact.name().equals(input.getTargetEntity())) {
                input.setDecisionGraph(defaultContactGraph);
            }
            if (StringUtils.isNotBlank(input.getDecisionGraph())) {
                log.debug(String.format("Did no specify decision graph for target entity %s, use default one %s",
                        input.getTargetEntity(), input.getDecisionGraph()));
            }
            return;
        }
        if (StringUtils.isBlank(input.getTargetEntity())) {
            DecisionGraph decisionGraph = findDecisionGraph(input);
            input.setTargetEntity(decisionGraph.getEntity());
        }
    }

    protected DecisionGraph findDecisionGraph(MatchInput input) {
        if (StringUtils.isBlank(input.getDecisionGraph())) {
            return null;
        }
        try {
            return matchDecisionGraphService.getDecisionGraph(input.getDecisionGraph());
        } catch (ExecutionException e) {
            throw new RuntimeException("Fail to find decision graph " + input.getDecisionGraph());
        }
    }

    public ColumnSelection parseColumnSelection(MatchInput input) {
        if (isAttrLookup(input)) {
            throw new UnsupportedOperationException("Should not call parseColumnSelection for cdl match.");
        } else {
            if (BusinessEntity.PrimeAccount.name().equals(input.getTargetEntity())) {
                //FIXME: should merge to column selection service after ingesting DataBlock metadata
                return input.getCustomSelection();
            } else {
                ColumnSelectionService columnSelectionService = beanDispatcher
                        .getColumnSelectionService(input.getDataCloudVersion());
                String dataCloudVersion = input.getDataCloudVersion();
                if (input.getUnionSelection() != null) {
                    return combineSelections(columnSelectionService, input.getUnionSelection(), dataCloudVersion);
                } else if (input.getPredefinedSelection() != null) {
                    return columnSelectionService.parsePredefinedColumnSelection(input.getPredefinedSelection(),
                            dataCloudVersion);
                } else {
                    return input.getCustomSelection();
                }
            }
        }
    }

    boolean isAttrLookup(MatchInput input) {
        CustomerSpace customerSpace = CustomerSpace.parse(input.getTenant().getId());
        return !OperationalMode.ENTITY_MATCH.equals(input.getOperationalMode())
                && !Boolean.TRUE.equals(input.getDataCloudOnly()) && zkConfigurationService.isCDLTenant(customerSpace);
    }

    public List<ColumnMetadata> parseCDLMetadata(MatchInput input) {
        if (isAttrLookup(input)) {
            return cdlColumnSelectionService.parseMetadata(input);
        } else {
            throw new UnsupportedOperationException("Should not call parseCDLMetadata for non-cdl match.");
        }
    }

    protected Pair<ColumnSelection, List<ColumnMetadata>> setAttrLookupMetadata(@NotNull MatchContext context,
            @NotNull MatchInput input, List<ColumnMetadata> metadatas) {
        if (metadatas == null) {
            metadatas = parseCDLMetadata(input);
        }
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = metadatas.stream() //
                .map(cm -> new Column(cm.getAttrName())) //
                .collect(Collectors.toList());
        columnSelection.setColumns(columns);
        context.setAccountLookupDataUnit(parseAccountLookupDataUnit(input));
        context.setCustomDataUnits(parseCustomDynamoDataUnits(input));
        return Pair.of(columnSelection, metadatas);
    }

    DynamoDataUnit parseAccountLookupDataUnit(MatchInput input) {
        return cdlColumnSelectionService.parseAccountLookupDataUnit(input);
    }

    List<DynamoDataUnit> parseCustomDynamoDataUnits(MatchInput input) {
        return cdlColumnSelectionService.parseCustomDynamoDataUnits(input);
    }

    public List<ColumnMetadata> parseEntityMetadata(MatchInput input) {
        switch (input.getPredefinedSelection()) {
        // AllocateId Mode
        case ID:
            if (BusinessEntity.Account.name().equals(input.getTargetEntity())) {
                List<ColumnMetadata> cms = new ArrayList<>(Arrays.asList( //
                        new ColumnMetadata(InterfaceName.EntityId.name(), String.class.getSimpleName()), //
                        new ColumnMetadata(InterfaceName.AccountId.name(), String.class.getSimpleName()), //
                        new ColumnMetadata(InterfaceName.LatticeAccountId.name(), String.class.getSimpleName()) //
                ));
                addLineageFields(input, cms);
                return cms;
            }
            if (BusinessEntity.Contact.name().equals(input.getTargetEntity())) {
                List<ColumnMetadata> cms = new ArrayList<>(Arrays.asList( //
                        new ColumnMetadata(InterfaceName.EntityId.name(), String.class.getSimpleName()), //
                        new ColumnMetadata(InterfaceName.ContactId.name(), String.class.getSimpleName()), //
                        new ColumnMetadata(InterfaceName.AccountId.name(), String.class.getSimpleName()), //
                        new ColumnMetadata(InterfaceName.LatticeAccountId.name(), String.class.getSimpleName()) //
                ));
                addLineageFields(input, cms);
                return cms;
            }
            throw new IllegalArgumentException("Unsupported entity " + input.getTargetEntity());
            // FetchOnly mode (Might be retired)
        case Seed:
            return Collections.singletonList(
                    new ColumnMetadata(InterfaceName.LatticeAccountId.name(), String.class.getSimpleName()));
        case LeadToAcct:
            return Collections
                    .singletonList(new ColumnMetadata(InterfaceName.AccountId.name(), String.class.getSimpleName()));
        default:
            throw new UnsupportedOperationException("Column Metadata parsing for non-ID case is unsupported.");

        }
    }

    @MatchStep(threshold = 100L)
    public ColumnSelection combineSelections(ColumnSelectionService columnSelectionService,
            UnionSelection unionSelection, String dataCloudVersion) {
        List<ColumnSelection> selections = new ArrayList<>();
        for (Map.Entry<Predefined, String> entry : unionSelection.getPredefinedSelections().entrySet()) {
            Predefined predefined = entry.getKey();
            selections.add(columnSelectionService.parsePredefinedColumnSelection(predefined, dataCloudVersion));
        }
        if (unionSelection.getCustomSelection() != null && !unionSelection.getCustomSelection().isEmpty()) {
            selections.add(unionSelection.getCustomSelection());
        }
        return ColumnSelection.combine(selections);
    }

    // this is a dispatcher method
    MatchContext scanInputData(MatchInput input, MatchContext context) {
        // Map<MatchKey, List<Integer>> keyPositionMap =
        // MatchKeyUtils.getKeyPositionMap(input);
        // TODO(jwinter): Put this back in after testing that
        // MatchKeyUtils.getKeyPositionMap works during testing.
        Map<MatchKey, List<Integer>> keyPositionMap = getKeyPositionMap(input);

        List<InternalOutputRecord> records = new ArrayList<>();
        Set<String> domainSet = new HashSet<>();
        Set<NameLocation> nameLocationSet = new HashSet<>();
        Set<String> keyFields = getKeyFields(input.getKeyMap());
        String lookupIdKey = getLookupIdKey(input.getKeyMap());
        for (int i = 0; i < input.getData().size(); i++) {
            InternalOutputRecord record = scanInputRecordAndUpdateKeySets(keyFields, input.getData().get(i), i,
                    input.getFields(), keyPositionMap, domainSet, nameLocationSet,
                    input.isPublicDomainAsNormalDomain());
            if (record != null) {
                record.setLookupIdKey(lookupIdKey);
                record.setColumnMatched(new ArrayList<>());
                records.add(record);
            }
        }

        context.setInternalResults(records);
        context.setDomains(domainSet);
        context.setNameLocations(nameLocationSet);

        return context;
    }

    MatchContext scanEntityInputData(MatchInput input, MatchContext context) {
        // EntityKeyMaps should be non-null, the EntityKeyMap is allowed to be empty now
        // since no fields are mandatory instantiate EntityKeyMap if it is not set
        if (!input.getEntityKeyMaps().containsKey(input.getTargetEntity())) {
            input.getEntityKeyMaps().put(input.getTargetEntity(), new MatchInput.EntityKeyMap());
        }
        Map<MatchKey, List<String>> keyMap = input.getEntityKeyMaps().get(input.getTargetEntity()).getKeyMap();
        if (MapUtils.isEmpty(keyMap)) {
            keyMap = new HashMap<>();
            input.getEntityKeyMaps().get(input.getTargetEntity()).setKeyMap(keyMap);
        }
        Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps = MatchKeyUtils.getEntityKeyPositionMaps(input);

        Tenant standardizedTenant = EntityMatchUtils.newStandardizedTenant(input.getTenant());
        List<InternalOutputRecord> records = new ArrayList<>();
        Set<String> keyFields = getKeyFields(keyMap);
        Pair<MatchKey, String> lookupPair = getEntityLookupIdKey(input.getOperationalMode(), keyMap);
        String lookupIdKey = lookupPair.getRight();
        for (int i = 0; i < input.getData().size(); i++) {
            InternalOutputRecord record = scanEntityInputRecordAndUpdateKeySets(keyFields, input.getData().get(i), i,
                    input, entityKeyPositionMaps, lookupPair.getLeft());
            record.setOrigTenant(input.getTenant());
            // NOTE tenant in match input should be already validated
            record.setParsedTenant(standardizedTenant);
            record.setLookupIdKey(lookupIdKey);
            record.setColumnMatched(new ArrayList<>());
            records.add(record);
        }
        context.setInternalResults(records);
        return context;
    }

    private void addLineageFields(@NotNull MatchInput input, @NotNull List<ColumnMetadata> cms) {
        if (input.isIncludeLineageFields()) {
            cms.add(new ColumnMetadata(InterfaceName.CDLCreatedTemplate.name(), String.class.getSimpleName()));
        }
    }

    private Set<String> getKeyFields(Map<MatchKey, List<String>> keyMap) {
        Set<String> keyFields = new HashSet<>();
        for (Map.Entry<MatchKey, List<String>> entry : keyMap.entrySet()) {
            keyFields.addAll(entry.getValue());
        }
        keyFields.add(InterfaceName.Id.toString());
        keyFields.add(InterfaceName.Event.toString());
        keyFields.add(InterfaceName.InternalId.toString());
        return keyFields;
    }

    /*-
     * Get Entity match attr lookup id key/value. Return Pair<MatchKeyForLookupId,
     * LookupIdKey>.
     *
     * NOTE: support LookupIdKey=AccountId only at the moment
     */
    private Pair<MatchKey, String> getEntityLookupIdKey(OperationalMode mode, Map<MatchKey, List<String>> keyMap) {
        if (MapUtils.isEmpty(keyMap)) {
            return Pair.of(null, null);
        }
        if (OperationalMode.ENTITY_MATCH_ATTR_LOOKUP.equals(mode) && keyMap.containsKey(MatchKey.EntityId)) {
            // look for EntityId match key
            return Pair.of(MatchKey.EntityId, InterfaceName.AccountId.name());
        } else {
            return Pair.of(null, null);
        }
    }

    /*
     * legacy function to retrieve lookup id key for CDL lookup
     */
    private String getLookupIdKey(Map<MatchKey, List<String>> keyMap) {
        if (MapUtils.isNotEmpty(keyMap) && keyMap.containsKey(MatchKey.LookupId)) {
            return keyMap.get(MatchKey.LookupId).get(0);
        } else {
            return null;
        }
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
        output.setEntityKeyMaps(input.getEntityKeyMaps());
        output.setKeyMap(input.getKeyMap());
        output.setSubmittedBy(input.getTenant());
        if (CollectionUtils.isNotEmpty(metadatas)) {
            output = appendMetadata(output, metadatas);
        } else {
            if (OperationalMode.ENTITY_MATCH.equals(input.getOperationalMode())) {
                throw new UnsupportedOperationException("Column metadatas should already be set for Entity Match");
            }
            if (BusinessEntity.PrimeAccount.name().equals(input.getTargetEntity())) {
                // FIXME: should move to metadata driven
                List<PrimeColumn> columns = DirectPlusUtils.getDataBlockMetadata();
                Set<String> requestedCols = new HashSet<>(columnSelection.getColumnIds());
                List<ColumnMetadata> cms = columns.stream().map(PrimeColumn::toColumnMetadata) //
                        .filter(cm -> requestedCols.contains(cm.getAttrName())) //
                        .collect(Collectors.toList());
                output.setMetadata(cms);
            } else {
                output = appendMetadata(output, columnSelection, input.getDataCloudVersion(), input.getMetadatas());
            }
        }
        output = parseOutputFields(output, input.getMetadataFields());
        MatchStatistics statistics = initializeStatistics(input);
        output.setStatistics(statistics);
        return output;
    }

    private static MatchStatistics initializeStatistics(MatchInput input) {
        MatchStatistics statistics = new MatchStatistics();
        statistics.setRowsRequested(input.getNumRows());
        return statistics;
    }

    private InternalOutputRecord scanInputRecordAndUpdateKeySets(Set<String> keyFields, List<Object> inputRecord,
            int rowNum, List<String> fields, Map<MatchKey, List<Integer>> keyPositionMap, Set<String> domainSet,
            Set<NameLocation> nameLocationSet, boolean treatPublicDomainAsNormal) {
        InternalOutputRecord record = new InternalOutputRecord();
        record.setRowNumber(rowNum);
        record.setMatched(false);
        record.setInput(inputRecord);
        int numInputFields = fields.size();
        if (inputRecord.size() != numInputFields) {
            record.setFailed(true);
            record.addErrorMessages("The number of objects in this row [" + inputRecord.size()
                    + "] does not match the number of fields claimed [" + numInputFields + "]");
            return record;
        }

        parseRecordForNameLocation(inputRecord, keyPositionMap, nameLocationSet, record);
        parseRecordForDuns(inputRecord, keyPositionMap, record);
        parseRecordForDomain(inputRecord, keyPositionMap, domainSet, treatPublicDomainAsNormal, record);
        parseRecordForLatticeAccountId(inputRecord, keyPositionMap, record);
        parseRecordForLookupId(inputRecord, keyPositionMap, record);
        profilingInputRecord(keyFields, inputRecord, fields, record);

        return record;
    }

    private InternalOutputRecord scanEntityInputRecordAndUpdateKeySets(Set<String> keyFields, List<Object> inputRecord,
            int rowNum, MatchInput input, Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps,
            MatchKey lookupIdKey) {
        InternalOutputRecord record = new InternalOutputRecord();
        record.setRowNumber(rowNum);
        record.setMatched(false);
        record.setInput(inputRecord);
        record.setEntityKeyPositionMap(entityKeyPositionMaps);
        int numInputFields = input.getFields().size();
        if (inputRecord.size() != numInputFields) {
            record.setFailed(true);
            record.addErrorMessages("The number of objects in this row [" + inputRecord.size()
                    + "] does not match the number of fields claimed [" + numInputFields + "]");
            return record;
        }
        parseRecordForLatticeAccountId(inputRecord, entityKeyPositionMaps.get(input.getTargetEntity()), record);
        parseRecordForEntityId(inputRecord, entityKeyPositionMaps.get(input.getTargetEntity()), record,
                MatchKey.EntityId.equals(lookupIdKey));
        if (MatchKey.LookupId.equals(lookupIdKey)) {
            // only set if EntityId match key is not provided by user
            parseRecordForLookupId(inputRecord, entityKeyPositionMaps.get(input.getTargetEntity()), record);
        }
        profilingInputRecord(keyFields, inputRecord, input.getFields(), record);

        return record;
    }

    private void profilingInputRecord(Set<String> keyFields, List<Object> inputRecord, List<String> fields,
            InternalOutputRecord record) {
        int numFeatureValue = 0;
        for (int i = 0; i < inputRecord.size(); i++) {
            if (!keyFields.contains(fields.get(i))) {
                if (inputRecord.get(i) != null) {
                    numFeatureValue++;
                }
            }
        }
        record.setNumFeatureValue(numFeatureValue);
    }

    private void parseRecordForDomain(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            Set<String> domainSet, boolean treatPublicDomainAsNormal, InternalOutputRecord record) {
        if (keyPositionMap.containsKey(MatchKey.Domain)) {
            boolean relaxPublicDomainCheck = isPublicDomainCheckRelaxed(record.getParsedNameLocation().getName(),
                    record.getParsedDuns());
            List<Integer> domainPosList = keyPositionMap.get(MatchKey.Domain);
            try {
                String cleanDomain = null;
                for (Integer domainPos : domainPosList) {
                    String originalDomain = (String) inputRecord.get(domainPos);
                    record.setOrigDomain(originalDomain);
                    cleanDomain = DomainUtils.parseDomain(originalDomain);
                    if (StringUtils.isNotEmpty(cleanDomain)) {
                        break;
                    }
                }
                record.setParsedDomain(cleanDomain);
                if (publicDomainService.isPublicDomain(cleanDomain)) {
                    // For match input with domain, but without name and duns,
                    // and domain is not in email format, public domain is
                    // treated as normal domain
                    if (treatPublicDomainAsNormal
                            || (relaxPublicDomainCheck && !DomainUtils.isEmail(record.getOrigDomain()))) {
                        record.setMatchEvenIsPublicDomain(true);
                        domainSet.add(cleanDomain);
                        record.addErrorMessages("Parsed to a public domain: " + cleanDomain
                                + ", but treat it as normal domain in match");
                    } else {
                        record.addErrorMessages("Parsed to a public domain: " + cleanDomain);
                    }
                    record.setPublicDomain(true);
                } else if (StringUtils.isNotEmpty(cleanDomain)) {
                    // update domain set
                    record.setPublicDomain(false);
                    domainSet.add(cleanDomain);
                }
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup domain field: " + e.getMessage());
            }
        }
    }

    private boolean isPublicDomainCheckRelaxed(String name, String duns) {
        return zkConfigurationService.isPublicDomainCheckRelaxed() && StringUtils.isBlank(name)
                && StringUtils.isBlank(duns);
    }

    private void parseRecordForNameLocation(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            Set<NameLocation> nameLocationSet, InternalOutputRecord record) {
        try {
            String originalName = null;
            if (keyPositionMap.containsKey(MatchKey.Name)) {
                List<Integer> namePosList = keyPositionMap.get(MatchKey.Name);
                for (Integer namePos : namePosList) {
                    originalName = (String) inputRecord.get(namePos);
                }
            }
            String originalCountry = null;
            if (keyPositionMap.containsKey(MatchKey.Country)) {
                List<Integer> countryPosList = keyPositionMap.get(MatchKey.Country);
                for (Integer countryPos : countryPosList) {
                    originalCountry = (String) inputRecord.get(countryPos);
                }
            }
            String originalState = null;
            if (keyPositionMap.containsKey(MatchKey.State)) {
                List<Integer> statePosList = keyPositionMap.get(MatchKey.State);
                for (Integer statePos : statePosList) {
                    originalState = (String) inputRecord.get(statePos);
                }
            }
            String originalCity = null;
            if (keyPositionMap.containsKey(MatchKey.City)) {
                for (Integer cityPos : keyPositionMap.get(MatchKey.City)) {
                    originalCity = (String) inputRecord.get(cityPos);
                }
            }
            String originalZipCode = null;
            if (keyPositionMap.containsKey(MatchKey.Zipcode)) {
                for (Integer pos : keyPositionMap.get(MatchKey.Zipcode)) {
                    if (inputRecord.get(pos) != null) {
                        if (inputRecord.get(pos) instanceof String) {
                            originalZipCode = (String) inputRecord.get(pos);
                        } else if (inputRecord.get(pos) instanceof Utf8 || inputRecord.get(pos) instanceof Long
                                || inputRecord.get(pos) instanceof Integer) {
                            originalZipCode = inputRecord.get(pos).toString();
                        }
                    }
                }
            }
            String originalPhoneNumber = null;
            if (keyPositionMap.containsKey(MatchKey.PhoneNumber)) {
                for (Integer pos : keyPositionMap.get(MatchKey.PhoneNumber)) {
                    if (inputRecord.get(pos) != null) {
                        if (inputRecord.get(pos) instanceof String) {
                            originalPhoneNumber = (String) inputRecord.get(pos);
                        } else if (inputRecord.get(pos) instanceof Utf8 || inputRecord.get(pos) instanceof Long
                                || inputRecord.get(pos) instanceof Integer) {
                            originalPhoneNumber = inputRecord.get(pos).toString();
                        }
                    }
                }
            }
            String originalStreet = null;
            if (keyPositionMap.containsKey(MatchKey.Address)) {
                for (Integer streetPos : keyPositionMap.get(MatchKey.Address)) {
                    originalStreet = (String) inputRecord.get(streetPos);
                }
            }
            String originalStreet2 = null;
            if (keyPositionMap.containsKey(MatchKey.Address2)) {
                for (Integer streetPos : keyPositionMap.get(MatchKey.Address2)) {
                    originalStreet2 = (String) inputRecord.get(streetPos);
                }
            }

            NameLocation origNameLocation = getNameLocation(originalName, originalCountry, originalState, originalCity,
                    originalZipCode, originalPhoneNumber, originalStreet, originalStreet2);
            record.setOrigNameLocation(origNameLocation);

            NameLocation nameLocation = getNameLocation(originalName, originalCountry, originalState, originalCity,
                    originalZipCode, originalPhoneNumber, originalStreet, originalStreet2);
            nameLocationService.normalize(nameLocation);
            record.setParsedNameLocation(nameLocation);
            if (isValidNameLocation(nameLocation)) {
                nameLocationSet.add(nameLocation);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            record.setFailed(true);
            record.addErrorMessages("Error when cleanup name and location fields: " + e.getMessage());
        }
    }

    private static boolean isValidNameLocation(NameLocation nameLocation) {
        return (StringUtils.isNotBlank(nameLocation.getName()) || StringUtils.isNotBlank(nameLocation.getPhoneNumber()))
                && StringUtils.isNotBlank(nameLocation.getCountryCode());
    }

    private NameLocation getNameLocation(String originalName, String originalCountry, String originalState,
            String originalCity, String originalZipCode, String originalPhoneNumber, String originalStreet, String originalStreet2) {
        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(originalName);
        nameLocation.setState(originalState);
        nameLocation.setCountry(originalCountry);
        nameLocation.setCity(originalCity);
        nameLocation.setZipcode(originalZipCode);
        nameLocation.setPhoneNumber(originalPhoneNumber);
        nameLocation.setStreet(originalStreet);
        nameLocation.setStreet2(originalStreet2);
        return nameLocation;
    }

    private void parseRecordForDuns(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            InternalOutputRecord record) {
        if (keyPositionMap.containsKey(MatchKey.DUNS)) {
            List<Integer> dunsPosList = keyPositionMap.get(MatchKey.DUNS);
            try {
                String cleanDuns = null;
                for (Integer dunsPos : dunsPosList) {
                    String originalDuns = inputRecord.get(dunsPos) == null ? null
                            : String.valueOf(inputRecord.get(dunsPos));
                    record.setOrigDuns(originalDuns);
                    if (StringUtils.isNotEmpty(originalDuns)) {
                        cleanDuns = StringStandardizationUtils.getStandardDuns(originalDuns);
                        break;
                    }
                }
                record.setParsedDuns(cleanDuns);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup duns field: " + e.getMessage());
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
                    String originalId = inputRecord.get(idPos) == null ? null : String.valueOf(inputRecord.get(idPos));
                    if (StringUtils.isNotEmpty(originalId)) {
                        cleanId = StringStandardizationUtils.getStandardizedInputLatticeID(originalId);
                        break;
                    }
                }
                record.setLatticeAccountId(cleanId);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup lattice account id field: " + e.getMessage());
            }
        }
    }

    private void parseRecordForLookupId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            InternalOutputRecord record) {
        if (keyPositionMap.containsKey(MatchKey.LookupId)) {
            List<Integer> idPosList = keyPositionMap.get(MatchKey.LookupId);
            Integer idPos = idPosList.get(0);
            try {
                String lookupId = inputRecord.get(idPos) == null ? null : String.valueOf(inputRecord.get(idPos));
                record.setLookupIdValue(lookupId);
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup lookup id field: " + e.getMessage());
            }
        }
    }

    private void parseRecordForEntityId(List<Object> inputRecord, Map<MatchKey, List<Integer>> keyPositionMap,
            InternalOutputRecord record, boolean setLookupIdValue) {
        if (keyPositionMap.containsKey(MatchKey.EntityId)) {
            List<Integer> idPosList = keyPositionMap.get(MatchKey.EntityId);
            try {
                String cleanId = null;
                for (Integer idPos : idPosList) {
                    String originalId = inputRecord.get(idPos) == null ? null : String.valueOf(inputRecord.get(idPos));
                    if (StringUtils.isNotEmpty(originalId)) {
                        cleanId = StringStandardizationUtils.getStandardizedSystemId(originalId, true);
                        break;
                    }
                }
                record.setEntityId(cleanId);
                if (setLookupIdValue) {
                    record.setLookupIdValue(cleanId);
                }
            } catch (Exception e) {
                record.setFailed(true);
                record.addErrorMessages("Error when cleanup lattice account id field: " + e.getMessage());
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
    private MatchOutput appendMetadata(MatchOutput matchOutput, ColumnSelection selection, String dataCloudVersion,
            List<ColumnMetadata> metadatas) {
        if (CollectionUtils.isEmpty(metadatas)) {
            ColumnMetadataService columnMetadataService = beanDispatcher.getColumnMetadataService(dataCloudVersion);
            metadatas = columnMetadataService.fromSelection(selection, dataCloudVersion);
        }
        matchOutput.setMetadata(metadatas);
        return matchOutput;
    }

    @MatchStep(threshold = 100L)
    private MatchOutput appendMetadata(MatchOutput matchOutput, List<ColumnMetadata> metadata) {
        matchOutput.setMetadata(metadata);
        return matchOutput;
    }

    private MatchOutput parseOutputFields(MatchOutput matchOutput, List<String> metadataFields) {
        if (CollectionUtils.isEmpty(metadataFields)) {
            List<ColumnMetadata> metadata = matchOutput.getMetadata();
            metadataFields = new ArrayList<>();
            for (ColumnMetadata column : metadata) {
                metadataFields.add(column.getAttrName());
            }
        }
        matchOutput.setOutputFields(metadataFields);
        return matchOutput;
    }

    void setZkConfigurationService(ZkConfigurationService zkConfigurationService) {
        this.zkConfigurationService = zkConfigurationService;
    }

}
