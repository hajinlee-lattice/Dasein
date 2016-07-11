package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchInput;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component("matcher)")
public class MatcherImpl implements Matcher {

    private static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    private static final Log log = LogFactory.getLog(MatcherImpl.class);

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private Warnings warnings;

    @Value("${scoringapi.pls.api.hostport}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    private MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary) {
        return buildMatchInput(space, interpreted, record, modelSummary, null);
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

        if (CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {
            if (modelSummary != null && modelSummary.getPredefinedSelection() != null) {
                matchInput.setPredefinedSelection(modelSummary.getPredefinedSelection());
                if (org.apache.commons.lang.StringUtils
                        .isNotEmpty(modelSummary.getPredefinedSelectionVersion())) {
                    matchInput.setPredefinedVersion(modelSummary.getPredefinedSelectionVersion());
                }
            } else {
                matchInput.setPredefinedSelection(
                        ColumnSelection.Predefined.getLegacyDefaultSelection());
            }
        } else {
            ColumnSelection customSelection = getCustomColumnSelection(
                    selectedLeadEnrichmentAttributes);
            matchInput.setCustomSelection(customSelection);
        }
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

    private ColumnSelection getCustomColumnSelection(
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        List<ExternalColumn> externalColumns = new ArrayList<>();

        for (LeadEnrichmentAttribute attr : selectedLeadEnrichmentAttributes) {
            ExternalColumn externalCol = new ExternalColumn();
            externalColumns.add(externalCol);
            externalCol.setCategory(Category.fromName(attr.getCategory()));
            externalCol.setDataType(attr.getFieldType());
            externalCol.setExternalColumnID(attr.getFieldName());
        }

        ColumnSelection customSelection = new ColumnSelection(externalColumns);
        return customSelection;
    }

    @Override
    public Map<String, Object> matchAndJoin(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            ModelSummary modelSummary) {
        MatchInput matchInput = buildMatchInput(space, interpreted, record, modelSummary);
        if (log.isDebugEnabled()) {
            log.debug("matchInput:" + JsonUtils.serialize(matchInput));
        }
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);
        if (log.isDebugEnabled()) {
            log.debug("matchOutput:" + JsonUtils.serialize(matchOutput));
        }

        getRecordFromMatchOutput(fieldSchemas, record, matchInput, matchOutput);
        return record;
    }

    private void getRecordFromMatchOutput(Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            MatchInput matchInput, //
            MatchOutput matchOutput) {
        if (matchOutput.getResult().isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.NO_MATCH,
                    new String[] { JsonUtils.serialize(matchInput.getKeyMap()), "No result" }));
        } else {
            List<String> matchFieldNames = matchOutput.getOutputFields();
            OutputRecord outputRecord = matchOutput.getResult().get(0);
            String nameLocationStr = "";
            if (outputRecord.getMatchedNameLocation() != null) {
                nameLocationStr = JsonUtils.serialize(outputRecord.getMatchedNameLocation());
            }
            String errorMessages = outputRecord.getErrorMessages() == null ? ""
                    : Joiner.on(",").join(outputRecord.getErrorMessages());

            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "{ 'isMatched':'%s', 'matchedDomain':'%s', 'matchedNameLocation':'%s', 'matchErrors':'%s' }",
                        outputRecord.isMatched(),
                        Strings.nullToEmpty(outputRecord.getMatchedDomain()), nameLocationStr,
                        errorMessages));
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
    public Map<RecordModelTuple, Map<String, Object>> matchAndJoin(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean forEnrichment) {
        BulkMatchInput matchInput = buildMatchInput(space, partiallyOrderedParsedTupleList,
                originalOrderModelSummaryList, forEnrichment);

        Map<RecordModelTuple, Map<String, Object>> results = new HashMap<>();

        if (!matchInput.getInputList().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("matchInput:" + JsonUtils.serialize(matchInput));
            }
            if (log.isInfoEnabled()) {
                log.info("Calling match for " + matchInput.getInputList().size() + " match inputs");
            }

            BulkMatchOutput matchOutput = matchProxy.matchRealTime(matchInput);
            if (log.isDebugEnabled()) {
                log.debug("matchOutput:" + JsonUtils.serialize(matchOutput));
            }
            if (log.isInfoEnabled()) {
                log.info("Completed match for " + matchInput.getInputList().size()
                        + " match inputs");
            }

            if (matchOutput != null) {
                int idx = 0;
                List<MatchOutput> outputList = matchOutput.getOutputList();
                for (MatchOutput output : outputList) {
                    String modelId = partiallyOrderedParsedTupleList.get(idx).getModelId();
                    Map<String, FieldSchema> fieldSchemas = uniqueFieldSchemasMap.get(modelId);

                    Map<String, Object> record = new HashMap<>();
                    getRecordFromMatchOutput(fieldSchemas, record,
                            matchInput.getInputList().get(idx), output);

                    results.put(partiallyOrderedParsedTupleList.get(idx), record);
                    idx++;
                }
            }
            if (log.isInfoEnabled()) {
                log.info("Completed post processing of matched result for "
                        + matchInput.getInputList().size() + " match inputs");
            }
        }
        return results;

    }

    private BulkMatchInput buildMatchInput(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean forEnrichment) {
        BulkMatchInput bulkInput = new BulkMatchInput();
        List<MatchInput> matchInputList = new ArrayList<>();
        bulkInput.setInputList(matchInputList);
        bulkInput.setRequestId(UUID.randomUUID().toString());
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;

        for (RecordModelTuple recordModelTuple : partiallyOrderedParsedTupleList) {
            ModelSummary modelSummary = getModelSummary(originalOrderModelSummaryList,
                    recordModelTuple.getModelId());

            if (forEnrichment && recordModelTuple.getRecord().isPerformEnrichment()) {
                if (selectedLeadEnrichmentAttributes == null) {
                    // we need to execute only once therefore null check is done
                    selectedLeadEnrichmentAttributes = internalResourceRestApiProxy
                            .getLeadEnrichmentAttributes(space, null, null, true);
                }

                if (!CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {
                    matchInputList
                            .add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                                    recordModelTuple.getParsedData().getKey(), modelSummary,
                                    selectedLeadEnrichmentAttributes));
                }
            } else {
                matchInputList
                        .add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                                recordModelTuple.getParsedData().getKey(), modelSummary));
            }
        }
        return bulkInput;
    }

    private ModelSummary getModelSummary(List<ModelSummary> modelSummaryList, //
            String modelId) {
        for (ModelSummary summary : modelSummaryList) {
            if (summary.getId().equals(modelId)) {
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
            throw new LedpException(LedpCode.LEDP_31005,
                    new String[] { String.valueOf(matchFieldNames.size()),
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
                        warnings.addWarning(new Warning(WarningCode.PUBLIC_DOMAIN, new String[] {
                                Strings.nullToEmpty(outputRecord.getMatchedDomain()) }));
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

}
