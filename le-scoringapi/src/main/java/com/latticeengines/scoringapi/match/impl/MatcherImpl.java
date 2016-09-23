package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.impl.RealTimeMatchFetcher;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component("matcher")
public class MatcherImpl implements Matcher, ApplicationContextAware {

    private static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    private static final Log log = LogFactory.getLog(MatcherImpl.class);

    @Autowired
    private Warnings warnings;

    private ApplicationContext applicationContext;

    @Autowired
    private RealTimeMatchFetcher realTimeMatchFetcher;

    @Value("${scoringapi.propdata.shortcircuit:false}")
    private boolean shouldShortcircuitPropdata;

    private List<RealTimeMatchService> realTimeMatchServiceList;

    @Autowired
    private MatchProxy matchProxy;

    @Value("${scoringapi.pls.api.hostport}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Value("${scoringapi.enrichment.cache.size:100}")
    private int maxEnrichmentCacheSize;

    @Value("${scoringapi.enrichment.cache.expiration.time:5}")
    private int enrichmentCacheExpirationTime;

    private LoadingCache<CustomerSpace, List<LeadEnrichmentAttribute>> leadEnrichmentAttributeCache;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        leadEnrichmentAttributeCache = //
        CacheBuilder.newBuilder()//
                .maximumSize(maxEnrichmentCacheSize)//
                .expireAfterWrite(enrichmentCacheExpirationTime, TimeUnit.MINUTES)//
                .build(new CacheLoader<CustomerSpace, List<LeadEnrichmentAttribute>>() {
                    public List<LeadEnrichmentAttribute> load(CustomerSpace customerSpace) throws Exception {
                        return internalResourceRestApiProxy
                                .getLeadEnrichmentAttributes(customerSpace, null, null, true);
                    }
                });

        if (shouldShortcircuitPropdata) {
            log.info("Initialize propdata fetcher executors as scoringapi-propdata shortcircuit is on.");
            Map<String, RealTimeMatchService> realTimeMatchServiceMap = //
            applicationContext//
                    .getBeansOfType(RealTimeMatchService.class, //
                            false, true);
            realTimeMatchServiceList = //
            new ArrayList<RealTimeMatchService>(realTimeMatchServiceMap.values());
            realTimeMatchFetcher.initExecutors();
        } else {
            log.info("Skip initialization of propdata fetcher executors "
                    + "via scoringapi as scoringapi-propdata shortcircuit is off.");
        }
    }

    private MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            boolean forEnrichment) {
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;
        if (forEnrichment) {
            selectedLeadEnrichmentAttributes = getEnrichmentAttributesMetadata(space, selectedLeadEnrichmentAttributes);
        }
        return buildMatchInput(space, interpreted, record, modelSummary, selectedLeadEnrichmentAttributes);
    }

    private MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        MatchInput matchInput = new MatchInput();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getEmailAddress(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getDomain(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getWebsite(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Name, interpreted.getCompanyName(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.City, interpreted.getCompanyCity(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.State, interpreted.getCompanyState(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Country, interpreted.getCompanyCountry(), record);
        matchInput.setKeyMap(keyMap);

        UnionSelection unionSelections = new UnionSelection();

        Map<Predefined, String> predefinedSelections = new HashMap<>();

        if (modelSummary != null && modelSummary.getPredefinedSelection() != null) {
            String version = null;
            if (org.apache.commons.lang.StringUtils.isNotEmpty(modelSummary.getPredefinedSelectionVersion())) {
                version = modelSummary.getPredefinedSelectionVersion();
            }
            predefinedSelections.put(modelSummary.getPredefinedSelection(), version);
        } else {
            predefinedSelections.put(Predefined.getLegacyDefaultSelection(), null);
        }
        unionSelections.setPredefinedSelections(predefinedSelections);
        if (modelSummary != null) {
            matchInput.setDataCloudVersion(modelSummary.getDataCloudVersion());

            if (modelSummary.getDataCloudVersion() != null && modelSummary.getDataCloudVersion().startsWith("2.")) {
                // TODO - work with Lei to fis RTS version to 2.0 during AM
                // based model creation
                predefinedSelections.put(modelSummary.getPredefinedSelection(), "2.0");
            }
        }

        if (!CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {
            ColumnSelection customSelection = getCustomColumnSelection(selectedLeadEnrichmentAttributes);
            unionSelections.setCustomSelection(customSelection);
        }

        matchInput.setUnionSelection(unionSelections);

        matchInput.setTenant(new Tenant(space.toString()));
        List<String> fields = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();
        List<Object> dataRecord = new ArrayList<>();
        data.add(dataRecord);
        for (String key : record.keySet()) {
            Object value = record.get(key);
            fields.add(key);
            dataRecord.add(value);
        }

        matchInput.setFields(fields);
        matchInput.setData(data);

        return matchInput;
    }

    private ColumnSelection getCustomColumnSelection(List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        List<ExternalColumn> externalColumns = new ArrayList<>();

        for (LeadEnrichmentAttribute attr : selectedLeadEnrichmentAttributes) {
            ExternalColumn externalCol = new ExternalColumn();
            externalColumns.add(externalCol);
            externalCol.setCategory(Category.fromName(attr.getCategory()));
            externalCol.setDataType(attr.getFieldType());
            externalCol.setExternalColumnID(attr.getFieldName());
            externalCol.setDefaultColumnName(attr.getFieldName());
        }

        ColumnSelection customSelection = new ColumnSelection();
        customSelection.createColumnSelection(externalColumns);
        return customSelection;
    }

    @Override
    public Map<String, Map<String, Object>> matchAndJoin(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            boolean forEnrichment) {
        MatchInput matchInput = buildMatchInput(space, interpreted, record, modelSummary, forEnrichment);
        if (log.isDebugEnabled()) {
            log.debug("matchInput:" + JsonUtils.serialize(matchInput));
        }

        MatchOutput matchOutput = null;

        if (shouldShortcircuitPropdata) {
            matchOutput = getRealTimeMatchService(matchInput.getDataCloudVersion()).match(matchInput);
        } else {
            matchOutput = matchProxy.matchRealTime(matchInput);
        }

        if (log.isDebugEnabled()) {
            log.debug("matchOutput:" + JsonUtils.serialize(matchOutput));
        }

        getRecordFromMatchOutput(fieldSchemas, record, matchInput, matchOutput);

        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        resultMap.put(RESULT, record);

        if (forEnrichment) {
            Map<String, Object> enrichmentData = new HashMap<>();
            if (matchInput.getUnionSelection() != null && matchInput.getUnionSelection().getCustomSelection() != null
                    && matchInput.getUnionSelection().getCustomSelection().getColumns() != null) {
                for (Column attr : matchInput.getUnionSelection().getCustomSelection().getColumns()) {
                    if (record.containsKey(attr.getExternalColumnId())) {
                        enrichmentData.put(attr.getExternalColumnId(), record.get(attr.getExternalColumnId()));
                    }
                }
            }
            resultMap.put(ENRICHMENT, enrichmentData);
        }

        return resultMap;
    }

    private void getRecordFromMatchOutput(Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            MatchInput matchInput, //
            MatchOutput matchOutput) {
        if (matchOutput.getResult().isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.NO_MATCH, new String[] {
                    JsonUtils.serialize(matchInput.getKeyMap()), "No result" }));
        } else {
            List<String> matchFieldNames = matchOutput.getOutputFields();
            OutputRecord outputRecord = matchOutput.getResult().get(0);
            String nameLocationStr = "";
            if (outputRecord.getMatchedNameLocation() != null) {
                nameLocationStr = JsonUtils.serialize(outputRecord.getMatchedNameLocation());
            }
            String errorMessages = outputRecord.getErrorMessages() == null ? "" : Joiner.on(",").join(
                    outputRecord.getErrorMessages());

            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "{ 'isMatched':'%s', 'matchedDomain':'%s', 'matchedNameLocation':'%s', 'matchErrors':'%s' }",
                        outputRecord.isMatched(), Strings.nullToEmpty(outputRecord.getMatchedDomain()),
                        nameLocationStr, errorMessages));
            }

            mergeMatchedOutput(matchFieldNames, outputRecord, fieldSchemas, record);
            if (!outputRecord.isMatched()) {
                warnings.addWarning(new Warning(WarningCode.NO_MATCH, new String[] {
                        JsonUtils.serialize(matchInput.getKeyMap()),
                        Strings.nullToEmpty(outputRecord.getMatchedDomain()) + nameLocationStr }));
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(JsonUtils.serialize(record));
        }
    }

    @Override
    public Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean isHomogeneous) {
        BulkMatchInput matchInput = buildMatchInput(space, partiallyOrderedParsedTupleList,
                originalOrderModelSummaryList, isHomogeneous);

        Map<RecordModelTuple, Map<String, Map<String, Object>>> results = new HashMap<>();

        if (!matchInput.getInputList().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("matchInput:" + JsonUtils.serialize(matchInput));
            }
            if (log.isInfoEnabled()) {
                log.info("Calling match for " + matchInput.getInputList().size() + " match inputs");
            }

            BulkMatchOutput matchOutput = null;

            if (shouldShortcircuitPropdata) {
                matchOutput = getRealTimeMatchService(matchInput.getInputList().get(0).getDataCloudVersion())
                        .matchBulk(matchInput);
            } else {
                matchOutput = matchProxy.matchRealTime(matchInput);
            }

            if (log.isDebugEnabled()) {
                log.debug("matchOutput:" + JsonUtils.serialize(matchOutput));
            }
            if (log.isInfoEnabled()) {
                log.info("Completed match for " + matchInput.getInputList().size() + " match inputs");
            }

            if (matchOutput != null) {
                postProcessMatchedResult(partiallyOrderedParsedTupleList, uniqueFieldSchemasMap, matchInput, results,
                        matchOutput);
            }
            if (log.isInfoEnabled()) {
                log.info("Completed post processing of matched result for " + matchInput.getInputList().size()
                        + " match inputs");
            }
        }

        return results;
    }

    private void postProcessMatchedResult(List<RecordModelTuple> partiallyOrderedParsedTupleList,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, BulkMatchInput matchInput,
            Map<RecordModelTuple, Map<String, Map<String, Object>>> results, BulkMatchOutput matchOutput) {
        int idx = 0;
        List<MatchOutput> outputList = matchOutput.getOutputList();
        for (MatchOutput output : outputList) {
            String modelId = partiallyOrderedParsedTupleList.get(idx).getModelId();
            Map<String, FieldSchema> fieldSchemas = uniqueFieldSchemasMap.get(modelId);

            Map<String, Object> matchedRecordResult = new HashMap<>(partiallyOrderedParsedTupleList.get(idx)
                    .getParsedData().getKey());
            getRecordFromMatchOutput(fieldSchemas, matchedRecordResult, matchInput.getInputList().get(idx), output);

            Map<String, Map<String, Object>> recordResultMap = new HashMap<>();
            recordResultMap.put(RESULT, matchedRecordResult);

            if (partiallyOrderedParsedTupleList.get(idx).getRecord().isPerformEnrichment()) {
                {
                    Map<String, Object> recordEnrichment = extractEnrichment(matchedRecordResult, matchInput
                            .getInputList().get(idx).getUnionSelection());
                    recordResultMap.put(ENRICHMENT, recordEnrichment);
                }
            }

            results.put(partiallyOrderedParsedTupleList.get(idx), recordResultMap);
            idx++;
        }
    }

    private Map<String, Object> extractEnrichment(Map<String, Object> matchedRecordResult, UnionSelection unionSelection) {
        Map<String, Object> recordEnrichment = new HashMap<>();
        if (unionSelection != null && unionSelection.getCustomSelection() != null) {
            ColumnSelection customSelection = unionSelection.getCustomSelection();
            for (String enrichmentCol : customSelection.getColumnNames()) {
                recordEnrichment.put(enrichmentCol, matchedRecordResult.get(enrichmentCol));
            }
        }
        return recordEnrichment;
    }

    private BulkMatchInput buildMatchInput(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean isHomogeneous) {
        BulkMatchInput bulkInput = new BulkMatchInput();
        List<MatchInput> matchInputList = new ArrayList<>();
        bulkInput.setInputList(matchInputList);
        bulkInput.setRequestId(UUID.randomUUID().toString());
        bulkInput.setHomogeneous(isHomogeneous);
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;

        for (RecordModelTuple recordModelTuple : partiallyOrderedParsedTupleList) {
            ModelSummary modelSummary = getModelSummary(originalOrderModelSummaryList, recordModelTuple.getModelId());

            if (recordModelTuple.getRecord().isPerformEnrichment()) {
                if (selectedLeadEnrichmentAttributes == null) {
                    // we need to execute only once therefore null check is done
                    selectedLeadEnrichmentAttributes = getEnrichmentAttributesMetadata(space,
                            selectedLeadEnrichmentAttributes);
                }

                if (!CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {

                    matchInputList.add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                            recordModelTuple.getParsedData().getKey(), modelSummary, selectedLeadEnrichmentAttributes));
                } else {
                    matchInputList.add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                            recordModelTuple.getParsedData().getKey(), modelSummary, null));
                }
            } else {
                matchInputList.add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(), recordModelTuple
                        .getParsedData().getKey(), modelSummary, null));
            }
        }
        return bulkInput;
    }

    private ModelSummary getModelSummary(List<ModelSummary> modelSummaryList, //
            String modelId) {
        for (ModelSummary summary : modelSummaryList) {
            if (summary != null && summary.getId() != null && summary.getId().equals(modelId)) {
                return summary;
            }
        }

        return null;
    }

    private void mergeMatchedOutput(List<String> matchFieldNames, //
            OutputRecord outputRecord, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record) {
        List<Object> matchFieldValues = outputRecord.getOutput();

        if (matchFieldNames.size() != matchFieldValues.size()) {
            throw new LedpException(LedpCode.LEDP_31005, new String[] { String.valueOf(matchFieldNames.size()),
                    String.valueOf(matchFieldValues.size()) });
        }

        for (int i = 0; i < matchFieldNames.size(); i++) {
            String fieldName = matchFieldNames.get(i);
            FieldSchema schema = fieldSchemas.get(fieldName);
            if (schema == null || (schema != null && schema.source != FieldSource.REQUEST)) {
                Object fieldValue = null;
                if (schema != null) {
                    fieldValue = FieldType.parse(schema.type, matchFieldValues.get(i));
                } else {
                    fieldValue = matchFieldValues.get(i);
                }
                record.put(fieldName, fieldValue);
                if (fieldName.equals(IS_PUBLIC_DOMAIN)) {
                    Boolean isPublicDomain = (Boolean) fieldValue;
                    if (isPublicDomain) {
                        warnings.addWarning(new Warning(WarningCode.PUBLIC_DOMAIN, new String[] { Strings
                                .nullToEmpty(outputRecord.getMatchedDomain()) }));
                    }
                }
            }
        }
    }

    private void addToKeyMapIfValueExists(Map<MatchKey, List<String>> keyMap, //
            MatchKey matchKey, //
            String field, //
            Map<String, Object> record) {
        Object value = record.get(field);

        if (StringUtils.objectIsNullOrEmptyString(value)) {
            return;
        }
        List<String> keyFields = keyMap.get(matchKey);
        if (keyFields == null) {
            keyFields = new ArrayList<>();
            keyMap.put(matchKey, keyFields);
        }
        keyFields.add(field);
    }

    private List<LeadEnrichmentAttribute> getEnrichmentAttributesMetadata(CustomerSpace space,
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        try {
            selectedLeadEnrichmentAttributes = leadEnrichmentAttributeCache.get(space);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31112, e, new String[] { e.getMessage() });
        }
        return selectedLeadEnrichmentAttributes;
    }

    private RealTimeMatchService getRealTimeMatchService(String matchVersion) {
        for (RealTimeMatchService handler : realTimeMatchServiceList) {
            if (handler.accept(matchVersion)) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_25021, new String[] { matchVersion });
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
