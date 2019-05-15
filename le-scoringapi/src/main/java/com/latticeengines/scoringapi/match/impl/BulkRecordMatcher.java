package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.scoringapi.score.AdditionalScoreConfig;
import com.latticeengines.scoringapi.score.BulkMatchingContext;
import com.latticeengines.scoringapi.score.SingleMatchingContext;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class BulkRecordMatcher extends AbstractMatcher {
    private static final Logger log = LoggerFactory.getLogger(BulkRecordMatcher.class);

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
    public Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(//
            AdditionalScoreConfig additionalScoreConfig, BulkMatchingContext bulkMatchingConfig,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, boolean shouldEnrichOnly) {
        Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap = //
                buildMatchInput(additionalScoreConfig, bulkMatchingConfig, partiallyOrderedParsedTupleList,
                        shouldEnrichOnly);

        Map<RecordModelTuple, Map<String, Map<String, Object>>> results = new HashMap<>();

        for (String key : matchInputMap.keySet()) {
            Pair<BulkMatchInput, List<RecordModelTuple>> pair = matchInputMap.get(key);

            if (CollectionUtils.isEmpty(pair.getValue())) {
                continue;
            }

            BulkMatchOutput matchOutput = executeMatch(pair.getKey(), additionalScoreConfig.isDebug());

            postProcessMatchOutput(pair, matchOutput, results, bulkMatchingConfig);
        }

        if (log.isInfoEnabled()) {
            log.info("Completed post processing of matched result for "//
                    + results.size() + " match inputs");
        }

        return results;
    }

    private void postProcessMatchOutput(//
            Pair<BulkMatchInput, List<RecordModelTuple>> pair, //
            BulkMatchOutput matchOutput, //
            Map<RecordModelTuple, Map<String, Map<String, Object>>> results, //
            BulkMatchingContext bulkMatchingConfig) {
        int idx = 0;

        List<MatchOutput> outputList = matchOutput.getOutputList();

        for (RecordModelTuple tuple : pair.getValue()) {
            List<String> matchLogs = new ArrayList<>();
            List<String> matchErrorLogs = new ArrayList<>();
            postProcessSingleMatchOutput(pair, results, bulkMatchingConfig.getUniqueFieldSchemasMap(), //
                    idx++, outputList, tuple, matchLogs, matchErrorLogs);
            bulkMatchingConfig.getUnorderedMatchLogMap().put(tuple, matchLogs);
            bulkMatchingConfig.getUnorderedMatchErrorLogMap().put(tuple, matchErrorLogs);
        }
    }

    private void postProcessSingleMatchOutput(//
            Pair<BulkMatchInput, List<RecordModelTuple>> pair, //
            Map<RecordModelTuple, Map<String, Map<String, Object>>> results, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            int idx, List<MatchOutput> outputList, RecordModelTuple tuple, //
            List<String> matchLogs, List<String> matchErrorLogs) {
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
        getRecordFromMatchOutput(fieldSchemas, matchedRecordResult, matchInput, tupleOutput, //
                matchLogs, matchErrorLogs, tuple.getRecord().getRecordId());

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

    private BulkMatchOutput executeMatch(BulkMatchInput matchInput, boolean isDebugMode) {
        log("matchInput:", matchInput, isDebugMode);

        if (isDebugMode && log.isInfoEnabled()) {
            log.info("Calling match for " + matchInput.getInputList().size() + " match inputs");
        }

        BulkMatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        log("matchOutput:", matchOutput, isDebugMode);

        if (isDebugMode && log.isInfoEnabled()) {
            log.info("Completed match for " + matchInput.getInputList().size() + " match inputs");
        }

        return matchOutput;
    }

    private Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> buildMatchInput(//
            AdditionalScoreConfig additionalScoreConfig, BulkMatchingContext bulkMatchingConfig,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            boolean shouldEnrichOnly) {
        Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap = //
                initializeMatchInputMap(additionalScoreConfig.isHomogeneous());

        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = getEnrichmentMetadata(
                additionalScoreConfig.getSpace(), partiallyOrderedParsedTupleList,
                additionalScoreConfig.isEnrichInternalAttributes());

        for (RecordModelTuple recordModelTuple : partiallyOrderedParsedTupleList) {
            prepareAndSetMatchInput(additionalScoreConfig, bulkMatchingConfig, partiallyOrderedParsedTupleList, //
                    matchInputMap, recordModelTuple, selectedLeadEnrichmentAttributes, //
                    shouldEnrichOnly);
        }

        return matchInputMap;
    }

    @Override
    public List<LeadEnrichmentAttribute> getEnrichmentMetadata(CustomerSpace space,
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

    private void prepareAndSetMatchInput(AdditionalScoreConfig additionalScoreConfig,
            BulkMatchingContext bulkMatchingConfig, List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Pair<BulkMatchInput, List<RecordModelTuple>>> matchInputMap, //
            RecordModelTuple recordModelTuple, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean shouldEnrichOnly) {
        ModelSummary modelSummary = getModelSummary(bulkMatchingConfig.getOriginalOrderModelSummaryList(),
                recordModelTuple.getModelId());

        boolean shouldCallEnrichmentExplicitly = //
                shouldCallEnrichmentExplicitly(modelSummary, //
                        recordModelTuple.getRecord().isPerformEnrichment(), //
                        selectedLeadEnrichmentAttributes);

        String currentDataCloudVersion = null;
        if (modelSummary != null && StringUtils.isNotBlank(modelSummary.getDataCloudVersion())) {
            currentDataCloudVersion = modelSummary.getDataCloudVersion() == null ? null
                    : columnMetadataProxy
                            .latestVersion(//
                                    modelSummary.getDataCloudVersion())//
                            .getVersion();
        }

        if (shouldEnrichOnly || shouldCallEnrichmentExplicitly) {
            // call regular match (without enrichment) if modelSummary is not
            // null
            if (!shouldEnrichOnly && modelSummary != null) {
                // IMP - make sure to not use performFetchOnlyForMatching for
                // RTS based lookup
                MatchInput matchOnlyInput = buildMatchInput(additionalScoreConfig.getSpace(), //
                        recordModelTuple.getParsedData().getValue(), //
                        recordModelTuple.getParsedData().getKey(), //
                        modelSummary, null, false, currentDataCloudVersion, //
                        false, additionalScoreConfig.getRequestId(), additionalScoreConfig.isDebug(), false, false);

                putInBulkMatchInput(RTS_MATCH_ONLY, matchInputMap, //
                        recordModelTuple, matchOnlyInput);
                if (additionalScoreConfig.isDebug()) {
                    log.info(String.format(
                            "Bulk-realtime match request info: recordId=%s, tenant=%s, scenario=%s, modelId=%s, dataCloudVersion=%s, rows=%d",
                            recordModelTuple.getRecord().getRecordId(), additionalScoreConfig.getSpace().getTenantId(),
                            RTS_MATCH_ONLY, modelSummary == null ? null : modelSummary.getId(),
                            matchOnlyInput.getDataCloudVersion(), matchOnlyInput.getData().size()));
                }
            }

            // call enrichment (without predefined column selection) against
            // AccountMaster only

            // for explicit enrichment call, use latest data cloud version (2.*)
            // by passing null in the api
            String currentDataCloudVersionForEnrichment = //
                    columnMetadataProxy
                            .latestVersion(//
                                    null)//
                            .getVersion();

            MatchInput matchAMEnrichmentInput = buildMatchInput(additionalScoreConfig.getSpace(), //
                    recordModelTuple.getParsedData().getValue(), //
                    recordModelTuple.getParsedData().getKey(), modelSummary, //
                    selectedLeadEnrichmentAttributes, //
                    true, currentDataCloudVersionForEnrichment, //
                    additionalScoreConfig.isPerformFetchOnlyForMatching(), additionalScoreConfig.getRequestId(),
                    additionalScoreConfig.isDebug(), false, false);

            putInBulkMatchInput(AM_ENRICH_ONLY, matchInputMap, //
                    recordModelTuple, matchAMEnrichmentInput);
            if (additionalScoreConfig.isDebug()) {
                log.info(String.format(
                        "Bulk-realtime match request info: recordId=%s, tenant=%s, scenario=%s, modelId=%s, dataCloudVersion=%s, rows=%d",
                        recordModelTuple.getRecord().getRecordId(), additionalScoreConfig.getSpace().getTenantId(),
                        AM_ENRICH_ONLY, modelSummary == null ? null : modelSummary.getId(),
                        matchAMEnrichmentInput.getDataCloudVersion(), matchAMEnrichmentInput.getData().size()));
            }
        } else {
            // call regular match
            MatchInput matchInput = buildMatchInput(additionalScoreConfig.getSpace(), //
                    recordModelTuple.getParsedData().getValue(), //
                    recordModelTuple.getParsedData().getKey(), modelSummary, //
                    recordModelTuple.getRecord().isPerformEnrichment() ? selectedLeadEnrichmentAttributes : null, false,
                    currentDataCloudVersion, additionalScoreConfig.isPerformFetchOnlyForMatching(),
                    additionalScoreConfig.getRequestId(), additionalScoreConfig.isDebug(), false, false);

            String key = RTS_MATCH_ONLY;
            if (modelSummary != null
                    && MatchTypeUtil.isValidForAccountMasterBasedMatch(modelSummary.getDataCloudVersion())) {
                key = AM_MATCH_AND_OR_ENRICH;
            }

            putInBulkMatchInput(key, matchInputMap, recordModelTuple, matchInput);
            if (additionalScoreConfig.isDebug()) {
                log.info(String.format(
                        "Bulk-realtime match request info: recordId=%s, tenant=%s, scenario=%s, modelId=%s, dataCloudVersion=%s, rows=%d",
                        recordModelTuple.getRecord().getRecordId(), additionalScoreConfig.getSpace().getTenantId(), key,
                        modelSummary == null ? null : modelSummary.getId(), matchInput.getDataCloudVersion(),
                        matchInput.getData().size()));
            }
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

    @Override
    public Map<String, Map<String, Object>> matchAndJoin(AdditionalScoreConfig additionalScoreConfig,
            SingleMatchingContext singleMatchingConfig, InterpretedFields interpreted, Map<String, Object> record,
            boolean forEnrichment) {
        throw new NotImplementedException("matchAndJoin is not implemented");
    }
}
