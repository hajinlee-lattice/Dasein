package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.match.MatchInputBuilder;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

public abstract class AbstractMatchInputBuilder implements MatchInputBuilder {

    // TODO - lei will remove useFuzzyMatch
    @Value("${datacloud.match.use.fuzzy.match:false}")
    private boolean useFuzzyMatch;

    @Override
    public MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            boolean performFetchOnlyForMatching, //
            String requestId) {
        return buildMatchInput(space, interpreted, //
                record, modelSummary, //
                selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, null, performFetchOnlyForMatching, requestId);
    }

    @Override
    public MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            String overrideDataCloudVersion, //
            boolean performFetchOnlyForMatching, //
            String requestId) {
        MatchInput matchInput = new MatchInput();

        setMatchKeyMap(interpreted, record, matchInput);

        setColumnSelections(modelSummary, selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, overrideDataCloudVersion, matchInput);

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

        matchInput.setFetchOnly(
                // TODO - lei will remove useFuzzyMatch
                useFuzzyMatch || performFetchOnlyForMatching);
        matchInput.setRootOperationUid(requestId);
        return matchInput;
    }

    @Override
    public BulkMatchInput buildMatchInput(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            List<ModelSummary> originalOrderModelSummaryList, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean isHomogeneous, //
            boolean skipPredefinedSelection, //
            boolean performFetchOnlyForMatching, //
            String requestId) {
        BulkMatchInput bulkInput = new BulkMatchInput();
        List<MatchInput> matchInputList = new ArrayList<>();
        bulkInput.setInputList(matchInputList);
        bulkInput.setRequestId(UUID.randomUUID().toString());
        bulkInput.setHomogeneous(isHomogeneous);

        for (RecordModelTuple recordModelTuple : partiallyOrderedParsedTupleList) {
            ModelSummary modelSummary = getModelSummary(originalOrderModelSummaryList, recordModelTuple.getModelId());

            if (recordModelTuple.getRecord().isPerformEnrichment()) {
                if (!CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {

                    matchInputList.add(//
                            buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                                    recordModelTuple.getParsedData().getKey(), modelSummary,
                                    selectedLeadEnrichmentAttributes, skipPredefinedSelection,
                                    performFetchOnlyForMatching, requestId));
                } else {
                    matchInputList.add(//
                            buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                                    recordModelTuple.getParsedData().getKey(), modelSummary, null,
                                    skipPredefinedSelection, performFetchOnlyForMatching, requestId));
                }
            } else {
                matchInputList.add(//
                        buildMatchInput(space, recordModelTuple.getParsedData().getValue(),
                                recordModelTuple.getParsedData().getKey(), modelSummary, null, //
                                skipPredefinedSelection, performFetchOnlyForMatching, requestId));
            }
        }
        return bulkInput;
    }

    protected void setColumnSelections(ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            String overrideDataCloudVersion, //
            MatchInput matchInput) {
        if (skipPredefinedSelection) {
            ColumnSelection customSelection = populateCustomSelection(selectedLeadEnrichmentAttributes);
            matchInput.setCustomSelection(customSelection);
        } else {
            setMatchUnionSelection(modelSummary, selectedLeadEnrichmentAttributes, matchInput);
        }

        setDataCloudVersion(modelSummary, overrideDataCloudVersion, matchInput);
    }

    protected void setDataCloudVersion(ModelSummary modelSummary, //
            String overrideDataCloudVersion, //
            MatchInput matchInput) {
        if (overrideDataCloudVersion != null) {
            matchInput.setDataCloudVersion(overrideDataCloudVersion);
        } else if (modelSummary != null) {
            matchInput.setDataCloudVersion(modelSummary.getDataCloudVersion());
        }
    }

    protected void setPredefinedSelections(ModelSummary modelSummary, Map<Predefined, String> predefinedSelections,
            UnionSelection unionSelections) {
        unionSelections.setPredefinedSelections(predefinedSelections);
    }

    private void setMatchKeyMap(InterpretedFields interpreted, Map<String, Object> record, MatchInput matchInput) {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getDomain(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getWebsite(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getEmailAddress(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Name, interpreted.getCompanyName(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.City, interpreted.getCompanyCity(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.State, interpreted.getCompanyState(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Country, interpreted.getCompanyCountry(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.DUNS, interpreted.getDuns(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Zipcode, interpreted.getPostalCode(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.PhoneNumber, interpreted.getPhoneNumber(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.LatticeAccountID, interpreted.getLatticeAccountId(), record);
        matchInput.setKeyMap(keyMap);
    }

    private void setMatchUnionSelection(ModelSummary modelSummary,
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, MatchInput matchInput) {
        UnionSelection unionSelections = new UnionSelection();

        Map<Predefined, String> predefinedSelections = getPredefinedSelection(modelSummary);
        setPredefinedSelections(modelSummary, predefinedSelections, unionSelections);

        ColumnSelection customSelection = populateCustomSelection(selectedLeadEnrichmentAttributes);
        unionSelections.setCustomSelection(customSelection);

        matchInput.setUnionSelection(unionSelections);
    }

    private Map<Predefined, String> getPredefinedSelection(ModelSummary modelSummary) {
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
        return predefinedSelections;
    }

    private ColumnSelection populateCustomSelection(List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        ColumnSelection customSelection = null;
        if (!CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {
            customSelection = getCustomColumnSelection(selectedLeadEnrichmentAttributes);
        }
        return customSelection;
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

    private ModelSummary getModelSummary(List<ModelSummary> modelSummaryList, //
            String modelId) {
        for (ModelSummary summary : modelSummaryList) {
            if (summary != null && summary.getId() != null && summary.getId().equals(modelId)) {
                return summary;
            }
        }

        return null;
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
}
