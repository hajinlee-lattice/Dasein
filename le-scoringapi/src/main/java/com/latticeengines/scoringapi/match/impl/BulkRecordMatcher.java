package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class BulkRecordMatcher extends AbstractMatcher {
    private static final Log log = LogFactory.getLog(BulkRecordMatcher.class);

    @Override
    public boolean accept(boolean isBulk) {
        return isBulk;
    }

    private MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        MatchInput matchInput = new MatchInput();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getDomain(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getWebsite(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getEmailAddress(), record);
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
        throw new NotImplementedException();
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
            logInDebugMode("matchInput:", matchInput);

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

            logInDebugMode("matchOutput:", matchOutput);

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

            Map<String, Object> matchedRecordResult = new HashMap<>(
                    partiallyOrderedParsedTupleList.get(idx).getParsedData().getKey());
            getRecordFromMatchOutput(fieldSchemas, matchedRecordResult, matchInput.getInputList().get(idx), output);

            Map<String, Map<String, Object>> recordResultMap = new HashMap<>();
            recordResultMap.put(RESULT, matchedRecordResult);

            if (partiallyOrderedParsedTupleList.get(idx).getRecord().isPerformEnrichment()) {
                {
                    Map<String, Object> recordEnrichment = extractEnrichment(matchedRecordResult,
                            matchInput.getInputList().get(idx).getUnionSelection());
                    recordResultMap.put(ENRICHMENT, recordEnrichment);
                }
            }

            results.put(partiallyOrderedParsedTupleList.get(idx), recordResultMap);
            idx++;
        }
    }

    private Map<String, Object> extractEnrichment(Map<String, Object> matchedRecordResult,
            UnionSelection unionSelection) {
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
                    selectedLeadEnrichmentAttributes = enrichmentMetadataCache.getEnrichmentAttributesMetadata(space);
                }

                if (!CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {

                    matchInputList.add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                            recordModelTuple.getParsedData().getKey(), modelSummary, selectedLeadEnrichmentAttributes));
                } else {
                    matchInputList.add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                            recordModelTuple.getParsedData().getKey(), modelSummary, null));
                }
            } else {
                matchInputList.add(buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                        recordModelTuple.getParsedData().getKey(), modelSummary, null));
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
