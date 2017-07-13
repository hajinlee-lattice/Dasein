package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class SingleRecordMatcher extends AbstractMatcher {
    protected static final Logger log = LoggerFactory.getLogger(SingleRecordMatcher.class);

    @Override
    public boolean accept(boolean isBulk) {
        return !isBulk;
    }

    @Override
    public Map<String, Map<String, Object>> matchAndJoin(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            boolean forEnrichment, //
            boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode, //
            List<String> matchLogs, List<String> matchErrorLogs, //
            boolean shouldReturnAllEnrichment) {
        return matchAndJoin(space, interpreted, fieldSchemas, record, modelSummary, forEnrichment,
                enrichInternalAttributes, performFetchOnlyForMatching, requestId, isDebugMode, matchLogs,
                matchErrorLogs, shouldReturnAllEnrichment, false, false);
    }

    @Override
    public Map<String, Map<String, Object>> matchAndJoin(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            boolean forEnrichment, //
            boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode, //
            List<String> matchLogs, List<String> matchErrorLogs, //
            boolean shouldReturnAllEnrichment, //
            boolean enforceFuzzyMatch, boolean skipDnBCache) {
        boolean shouldCallEnrichmentExplicitly = false;
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;

        if (forEnrichment) {
            selectedLeadEnrichmentAttributes = getEnrichmentMetadata(space, enrichInternalAttributes,
                    shouldReturnAllEnrichment);

            if (modelSummary != null) {
                shouldCallEnrichmentExplicitly = shouldCallEnrichmentExplicitly(modelSummary, //
                        forEnrichment, selectedLeadEnrichmentAttributes);
            }
        }

        String currentDataCloudVersion = null;

        if (modelSummary == null) {
            // this means only enrichment is needed
            currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        } else if (StringUtils.isNotBlank(modelSummary.getDataCloudVersion())) {
            currentDataCloudVersion = modelSummary.getDataCloudVersion() == null ? null
                    : columnMetadataProxy
                            .latestVersion(//
                                    modelSummary.getDataCloudVersion())//
                            .getVersion();
        }
        
        if (shouldCallEnrichmentExplicitly) {
            Map<String, Map<String, Object>> result = new HashMap<>();
            // call regular match (without enrichment) if modelSummary is not
            // null
            if (modelSummary != null) {
                // IMP - make sure to not use performFetchOnlyForMatching for
                // RTS based lookup
                Map<String, Map<String, Object>> matchResult = //
                        buildAndExecuteMatch(space, interpreted, //
                                fieldSchemas, record, //
                                modelSummary, false, //
                                null, false, currentDataCloudVersion, //
                                false, requestId, isDebugMode, //
                                matchLogs, matchErrorLogs);
                result.putAll(matchResult);
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

            Map<String, Map<String, Object>> enrichmentResult = //
                    buildAndExecuteMatch(space, interpreted, //
                            fieldSchemas, record, //
                            null, true, //
                            selectedLeadEnrichmentAttributes, true, //
                            currentDataCloudVersionForEnrichment, performFetchOnlyForMatching, //
                            requestId, isDebugMode, //
                            matchLogs, matchErrorLogs, //
                            enforceFuzzyMatch, skipDnBCache);

            result.putAll(enrichmentResult);

            return result;
        } else {
            // call regular match
            return buildAndExecuteMatch(space, interpreted, fieldSchemas, //
                    record, modelSummary, forEnrichment, //
                    selectedLeadEnrichmentAttributes, (modelSummary == null), currentDataCloudVersion, //
                    performFetchOnlyForMatching, requestId, isDebugMode, //
                    matchLogs, matchErrorLogs, enforceFuzzyMatch, skipDnBCache);
        }
    }

    @Override
    public Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(//
            CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean isHomogeneous, //
            boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            boolean shouldEnrichOnly, //
            boolean isDebugMode, //
            String requestId, //
            Map<RecordModelTuple, List<String>> matchLogMap, //
            Map<RecordModelTuple, List<String>> matchErrorLogMap) {
        throw new NotImplementedException("matchAndJoin is not implemented");
    }

    private Map<String, Map<String, Object>> buildAndExecuteMatch(//
            CustomerSpace space, InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, //
            ModelSummary modelSummary, boolean forEnrichment, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, String overrideDataCloudVersion, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode, //
            List<String> matchLogs, List<String> matchErrorLogs) {
        return buildAndExecuteMatch(space, interpreted, fieldSchemas, //
                record, modelSummary, forEnrichment, selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, overrideDataCloudVersion, //
                performFetchOnlyForMatching, requestId, isDebugMode, matchLogs, //
                matchErrorLogs, false, false);
    }

    private Map<String, Map<String, Object>> buildAndExecuteMatch(//
            CustomerSpace space, InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, //
            ModelSummary modelSummary, boolean forEnrichment, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, String overrideDataCloudVersion, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode, //
            List<String> matchLogs, List<String> matchErrorLogs, //
            boolean enforceFuzzyMatch, boolean skipDnBCache) {
        MatchInput matchInput = buildMatchInput(space, interpreted, //
                record, modelSummary, //
                selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, overrideDataCloudVersion, //
                performFetchOnlyForMatching, requestId, isDebugMode, enforceFuzzyMatch, skipDnBCache);

        MatchOutput matchOutput = callMatch(matchInput, isDebugMode);

        getRecordFromMatchOutput(fieldSchemas, record, matchInput, matchOutput, matchLogs, matchErrorLogs);

        Map<String, Map<String, Object>> resultMap = new HashMap<>();

        if (!skipPredefinedSelection) {
            resultMap.put(RESULT, record);
        }

        doEnrichmentPostProcessing(record, forEnrichment, matchInput, resultMap);

        return resultMap;
    }

    private MatchOutput callMatch(MatchInput matchInput, boolean isDebugMode) {
        log("matchInput:", matchInput, isDebugMode);

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        log("matchOutput:", matchOutput, isDebugMode);

        return matchOutput;
    }

    private List<LeadEnrichmentAttribute> getEnrichmentMetadata(CustomerSpace space, boolean enrichInternalAttributes,
            boolean shouldReturnAllEnrichment) {
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = new ArrayList<>();
        List<LeadEnrichmentAttribute> tempSelectedLeadEnrichmentAttributes = null;
        if (shouldReturnAllEnrichment) {
            tempSelectedLeadEnrichmentAttributes = enrichmentMetadataCache.getAllEnrichmentAttributesMetadata();
        } else {
            tempSelectedLeadEnrichmentAttributes = enrichmentMetadataCache.getEnrichmentAttributesMetadata(space);
        }

        for (LeadEnrichmentAttribute attr : tempSelectedLeadEnrichmentAttributes) {
            if (enrichInternalAttributes || !attr.getIsInternal()) {
                selectedLeadEnrichmentAttributes.add(attr);
            }
        }

        return selectedLeadEnrichmentAttributes;
    }
}
