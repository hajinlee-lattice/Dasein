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
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.AdditionalScoreConfig;
import com.latticeengines.scoringapi.score.BulkMatchingContext;
import com.latticeengines.scoringapi.score.SingleMatchingContext;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class SingleRecordMatcher extends AbstractMatcher {
    protected static final Logger log = LoggerFactory.getLogger(SingleRecordMatcher.class);

    @Override
    public boolean accept(boolean isBulk) {
        return !isBulk;
    }

    @Override
    public Map<String, Map<String, Object>> matchAndJoin(AdditionalScoreConfig additionalScoreConfig,
            SingleMatchingContext singleMatchingConfig, InterpretedFields interpreted, Map<String, Object> record,
            boolean forEnrichment) {
        boolean shouldCallEnrichmentExplicitly = false;
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;
        ModelSummary modelSummary = singleMatchingConfig.getModelSummary();

        if (forEnrichment) {
            selectedLeadEnrichmentAttributes = getEnrichmentMetadata(additionalScoreConfig.getSpace(),
                    additionalScoreConfig.isEnrichInternalAttributes(),
                    additionalScoreConfig.isShouldReturnAllEnrichment());

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
                        buildAndExecuteMatch(additionalScoreConfig, singleMatchingConfig, interpreted, //
                                record, false, null, false, currentDataCloudVersion);
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
                    buildAndExecuteMatch(additionalScoreConfig, singleMatchingConfig, interpreted, //
                            record, //
                            true, //
                            selectedLeadEnrichmentAttributes, true, //
                            currentDataCloudVersionForEnrichment);

            result.putAll(enrichmentResult);

            return result;
        } else {
            // call regular match
            return buildAndExecuteMatch(additionalScoreConfig, singleMatchingConfig, interpreted, record, forEnrichment, //
                    selectedLeadEnrichmentAttributes, (modelSummary == null), currentDataCloudVersion);
        }
    }

    private Map<String, Map<String, Object>> buildAndExecuteMatch(//
            AdditionalScoreConfig additionalScoreConfig, SingleMatchingContext singleMatchingConfig,
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            boolean forEnrichment, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, boolean skipPredefinedSelection,
            String overrideDataCloudVersion //
    ) {
        MatchInput matchInput = buildMatchInput(additionalScoreConfig.getSpace(), interpreted, //
                record, singleMatchingConfig.getModelSummary(), //
                selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, overrideDataCloudVersion, //
                additionalScoreConfig.isPerformFetchOnlyForMatching(), additionalScoreConfig.getRequestId(),
                additionalScoreConfig.isDebug(), additionalScoreConfig.isEnforceFuzzyMatch(),
                additionalScoreConfig.isSkipDnBCache());

        MatchOutput matchOutput = callMatch(matchInput, additionalScoreConfig.isDebug());

        getRecordFromMatchOutput(singleMatchingConfig.getFieldSchemas(), record, matchInput, matchOutput,
                singleMatchingConfig.getMatchLogs(), singleMatchingConfig.getMatchErrorLogs(),
                additionalScoreConfig.getRequestId());

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
            tempSelectedLeadEnrichmentAttributes = internalResourceRestApiProxy.getAllLeadEnrichmentAttributes();
        } else {
            tempSelectedLeadEnrichmentAttributes = internalResourceRestApiProxy.getLeadEnrichmentAttributes(space, null,
                    null, true, true);
        }

        for (LeadEnrichmentAttribute attr : tempSelectedLeadEnrichmentAttributes) {
            if (enrichInternalAttributes || !attr.getIsInternal()) {
                selectedLeadEnrichmentAttributes.add(attr);
            }
        }

        return selectedLeadEnrichmentAttributes;
    }

    @Override
    public Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(//
            AdditionalScoreConfig additionalScoreConfig, BulkMatchingContext bulkMatchingConfig,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, boolean shouldEnrichOnly) {
        throw new NotImplementedException("matchAndJoin is not implemented");
    }

    @Override
    public List<LeadEnrichmentAttribute> getEnrichmentMetadata(CustomerSpace space,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, boolean enrichInternalAttributes) {
        throw new NotImplementedException("matchAndJoin is not implemented");
    }
}
