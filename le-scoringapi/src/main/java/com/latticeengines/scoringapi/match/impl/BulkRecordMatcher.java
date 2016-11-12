package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class BulkRecordMatcher extends AbstractMatcher {
    private static final Log log = LogFactory.getLog(BulkRecordMatcher.class);

    private static final String RTS_MATCH_ONLY = "RTS_MATCH_ONLY";
    private static final String AM_ENRICH_ONLY = "AM_ENRICH_ONLY";
    private static final String AM_MATCH_AND_OR_ENRICH = "AM_MATCH_AND_OR_ENRICH";

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public boolean accept(boolean isBulk) {
        return isBulk;
    }

    @Override
    public Map<String, Map<String, Object>> matchAndJoin(//
            CustomerSpace space, InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            boolean forEnrichment, //
            boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            String requestId) {
        throw new NotImplementedException();
    }

    @Override
    public Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(//
            CustomerSpace space, List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean isHomogeneous, //
            boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            String requestId) {
        Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap = //
                buildMatchInput(space, partiallyOrderedParsedTupleList, uniqueFieldSchemasMap,
                        originalOrderModelSummaryList, isHomogeneous, enrichInternalAttributes,
                        performFetchOnlyForMatching, requestId);

        Map<RecordModelTuple, Map<String, Map<String, Object>>> results = new HashMap<>();

        for (String key : matchInputMap.keySet()) {
            Pair<BulkMatchInput, List<RecordModelTuple>> pair = matchInputMap.get(key);

            if (CollectionUtils.isEmpty(pair.getValue())) {
                continue;
            }

            BulkMatchOutput matchOutput = executeMatch(pair.getKey());

            postProcessMatchOutput(pair, matchOutput, results, uniqueFieldSchemasMap);
        }

        if (log.isInfoEnabled()) {
            log.info("Completed post processing of matched result for "//
                    + results.size() + " match inputs");
        }

        return results;
    }

    private void postProcessMatchOutput(Pair<BulkMatchInput, List<RecordModelTuple>> pair, //
            BulkMatchOutput matchOutput, //
            Map<RecordModelTuple, Map<String, Map<String, Object>>> results, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap) {
        int idx = 0;

        List<MatchOutput> outputList = matchOutput.getOutputList();

        for (RecordModelTuple tuple : pair.getValue()) {
            postProcessSingleMatchOutput(pair, results, uniqueFieldSchemasMap, //
                    idx++, outputList, tuple);
        }
    }

    private void postProcessSingleMatchOutput(Pair<BulkMatchInput, List<RecordModelTuple>> pair,
            Map<RecordModelTuple, Map<String, Map<String, Object>>> results,
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, int idx, List<MatchOutput> outputList,
            RecordModelTuple tuple) {
        Map<String, Map<String, Object>> tupleResult = results.get(tuple);
        if (tupleResult == null) {
            tupleResult = new HashMap<>();
            results.put(tuple, tupleResult);
        }

        MatchInput matchInput = pair.getKey().getInputList().get(idx);
        MatchOutput tupleOutput = outputList.get(idx);

        String modelId = tuple.getModelId();

        Map<String, FieldSchema> fieldSchemas = uniqueFieldSchemasMap.get(modelId);

        Map<String, Object> matchedRecordResult = new HashMap<>(tuple.getParsedData().getKey());
        getRecordFromMatchOutput(fieldSchemas, matchedRecordResult, matchInput, tupleOutput);

        boolean setEnrichmentData = false;
        if (matchInput.getUnionSelection() != null) {
            tupleResult.put(RESULT, matchedRecordResult);

            if (matchInput.getUnionSelection().getCustomSelection() != null) {
                setEnrichmentData = true;
            }
        } else if (matchInput.getCustomSelection() != null) {
            setEnrichmentData = true;
        }

        doEnrichmentPostProcessing(matchedRecordResult, setEnrichmentData, //
                matchInput, tupleResult);
    }

    private BulkMatchOutput executeMatch(BulkMatchInput matchInput) {
        logInDebugMode("matchInput:", matchInput);

        if (log.isInfoEnabled()) {
            log.info("Calling match for " + matchInput.getInputList().size() + " match inputs");
        }

        BulkMatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        logInDebugMode("matchOutput:", matchOutput);

        if (log.isInfoEnabled()) {
            log.info("Completed match for " + matchInput.getInputList().size() + " match inputs");
        }

        return matchOutput;
    }

    private Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> buildMatchInput(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean isHomogeneous, boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            String requestId) {
        Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap = //
                initializeMatchInputMap(isHomogeneous);

        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = getEnrichmentMetadata(space,
                partiallyOrderedParsedTupleList, enrichInternalAttributes);

        for (RecordModelTuple recordModelTuple : partiallyOrderedParsedTupleList) {
            prepareAndSetMatchInput(space, partiallyOrderedParsedTupleList, uniqueFieldSchemasMap,
                    originalOrderModelSummaryList, matchInputMap, recordModelTuple, selectedLeadEnrichmentAttributes,
                    enrichInternalAttributes, performFetchOnlyForMatching, requestId);
        }

        return matchInputMap;
    }

    private List<LeadEnrichmentAttribute> getEnrichmentMetadata(CustomerSpace space,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, boolean enrichInternalAttributes) {
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;

        for (RecordModelTuple recordModelTuple : partiallyOrderedParsedTupleList) {
            if (recordModelTuple.getRecord().isPerformEnrichment()) {
                selectedLeadEnrichmentAttributes = new ArrayList<>();
                List<LeadEnrichmentAttribute> tempSelectedLeadEnrichmentAttributes = enrichmentMetadataCache
                        .getEnrichmentAttributesMetadata(space);
                for (LeadEnrichmentAttribute attr : tempSelectedLeadEnrichmentAttributes) {
                    if (enrichInternalAttributes || !attr.getIsInternal()) {
                        selectedLeadEnrichmentAttributes.add(attr);
                    }
                }

                break;
            }
        }

        return selectedLeadEnrichmentAttributes;
    }

    private void prepareAndSetMatchInput(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap, //
            RecordModelTuple recordModelTuple, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, //
            String requestId) {
        ModelSummary modelSummary = getModelSummary(originalOrderModelSummaryList, recordModelTuple.getModelId());

        boolean shouldCallEnrichmentExplicitly = //
                shouldCallEnrichmentExplicitly(modelSummary, //
                        recordModelTuple.getRecord().isPerformEnrichment(), //
                        selectedLeadEnrichmentAttributes);

        if (shouldCallEnrichmentExplicitly) {
            // call regular match (without enrichment) if modelSummary is not
            // null
            if (modelSummary != null) {
                MatchInput matchOnlyInput = buildMatchInput(space, //
                        recordModelTuple.getParsedData().getValue(), //
                        recordModelTuple.getParsedData().getKey(), //
                        modelSummary, null, false, null, //
                        performFetchOnlyForMatching, requestId);

                putInBulkMatchInput(RTS_MATCH_ONLY, matchInputMap, recordModelTuple, matchOnlyInput);
            }

            // call enrichment (without predefined column selection) against
            // AccountMaster only
            String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
            MatchInput matchAMEnrichmentInput = buildMatchInput(space, recordModelTuple.getParsedData().getValue(), //
                    recordModelTuple.getParsedData().getKey(), modelSummary, //
                    selectedLeadEnrichmentAttributes, //
                    true, currentDataCloudVersion, //
                    performFetchOnlyForMatching, requestId);

            putInBulkMatchInput(AM_ENRICH_ONLY, matchInputMap, recordModelTuple, matchAMEnrichmentInput);
        } else {
            // call regular match
            MatchInput matchInput = buildMatchInput(space, //
                    recordModelTuple.getParsedData().getValue(), //
                    recordModelTuple.getParsedData().getKey(), modelSummary, //
                    recordModelTuple.getRecord().isPerformEnrichment() ? selectedLeadEnrichmentAttributes : null, false,
                    null, performFetchOnlyForMatching, requestId);

            String key = RTS_MATCH_ONLY;
            if (modelSummary != null
                    && MatchTypeUtil.isValidForAccountMasterBasedMatch(modelSummary.getDataCloudVersion())) {
                key = AM_MATCH_AND_OR_ENRICH;
            }

            putInBulkMatchInput(key, matchInputMap, recordModelTuple, matchInput);
        }
    }

    private void putInBulkMatchInput(//
            String key, Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap, //
            RecordModelTuple recordModelTuple, MatchInput matchOnlyInput) {
        Pair<BulkMatchInput, List<RecordModelTuple>> pair = matchInputMap.get(key);

        BulkMatchInput bulkMatchInput = pair.getKey();

        List<RecordModelTuple> recordModelTupleList = pair.getValue();
        if (recordModelTupleList == null) {
            recordModelTupleList = new ArrayList<RecordModelTuple>();
            pair.setValue(recordModelTupleList);
        }

        List<MatchInput> matchInputList = bulkMatchInput.getInputList();
        if (matchInputList == null) {
            matchInputList = new ArrayList<>();
            bulkMatchInput.setInputList(matchInputList);
        }

        matchInputList.add(matchOnlyInput);
        recordModelTupleList.add(recordModelTuple);
    }

    private Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> initializeMatchInputMap(//
            boolean isHomogeneous) {
        Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap = new HashMap<>();

        BulkMatchInput bulkInputWithRTSMatchOnly = new BulkMatchInput();
        BulkMatchInput bulkInputWithAMEnrichOnly = new BulkMatchInput();
        BulkMatchInput bulkInputWithAMMatchAndEnrich = new BulkMatchInput();
        List<RecordModelTuple> tuplesForRTSMatchOnly = null;
        List<RecordModelTuple> tuplesForAMEnrichOnly = null;
        List<RecordModelTuple> tuplesForAMMatchAndEnrich = null;

        populateMatchInputMap(RTS_MATCH_ONLY, bulkInputWithRTSMatchOnly, //
                tuplesForRTSMatchOnly, matchInputMap, isHomogeneous);
        populateMatchInputMap(AM_ENRICH_ONLY, bulkInputWithAMEnrichOnly, //
                tuplesForAMEnrichOnly, matchInputMap, isHomogeneous);
        populateMatchInputMap(AM_MATCH_AND_OR_ENRICH, bulkInputWithAMMatchAndEnrich, //
                tuplesForAMMatchAndEnrich, matchInputMap, isHomogeneous);
        return matchInputMap;
    }

    private void populateMatchInputMap(String key, BulkMatchInput bulkMatchInput,
            List<RecordModelTuple> tuplesForRTSMatchOnly,
            Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap, //
            boolean isHomogeneous) {
        Pair<BulkMatchInput, List<RecordModelTuple>> pair = //
                new MutablePair<BulkMatchInput, //
                List<RecordModelTuple>>(bulkMatchInput, //
                        tuplesForRTSMatchOnly);
        matchInputMap.put(key, pair);
        bulkMatchInput.setHomogeneous(isHomogeneous);
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
}
