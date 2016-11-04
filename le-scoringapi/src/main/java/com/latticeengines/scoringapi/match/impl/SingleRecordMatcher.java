package com.latticeengines.scoringapi.match.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class SingleRecordMatcher extends AbstractMatcher {
    protected static final Log log = LogFactory.getLog(SingleRecordMatcher.class);

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

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
            boolean forEnrichment) {
        boolean shouldCallEnrichmentExplicitly = false;
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;

        if (forEnrichment) {
            selectedLeadEnrichmentAttributes = //
                    enrichmentMetadataCache.getEnrichmentAttributesMetadata(space);

            shouldCallEnrichmentExplicitly = shouldCallEnrichmentExplicitly(modelSummary, //
                    forEnrichment, selectedLeadEnrichmentAttributes);
        }

        if (shouldCallEnrichmentExplicitly) {
            Map<String, Map<String, Object>> result = new HashMap<>();
            // call regular match (without enrichment) if modelSummary is not
            // null
            if (modelSummary != null) {
                Map<String, Map<String, Object>> matchResult = //
                        buildAndExecuteMatch(space, interpreted, //
                                fieldSchemas, record, //
                                modelSummary, false, //
                                null, //
                                false, null);
                result.putAll(matchResult);
            }

            // call enrichment (without predefined column selection) against
            // AccountMaster only
            String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
            Map<String, Map<String, Object>> enrichmentResult = //
                    buildAndExecuteMatch(space, interpreted, //
                            fieldSchemas, record, //
                            null, true, //
                            selectedLeadEnrichmentAttributes, true, //
                            currentDataCloudVersion);

            result.putAll(enrichmentResult);

            return result;
        } else {
            // call regular match
            return buildAndExecuteMatch(space, interpreted, fieldSchemas, //
                    record, modelSummary, forEnrichment, //
                    selectedLeadEnrichmentAttributes, false, null);
        }
    }

    @Override
    public Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(//
            CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap, //
            List<ModelSummary> originalOrderModelSummaryList, //
            boolean isHomogeneous) {
        throw new NotImplementedException();
    }

    private Map<String, Map<String, Object>> buildAndExecuteMatch(//
            CustomerSpace space, InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, //
            ModelSummary modelSummary, boolean forEnrichment, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, String overrideDataCloudVersion) {
        MatchInput matchInput = buildMatchInput(space, interpreted, //
                record, modelSummary, //
                selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, overrideDataCloudVersion);

        MatchOutput matchOutput = callMatch(matchInput);

        getRecordFromMatchOutput(fieldSchemas, record, matchInput, matchOutput);

        Map<String, Map<String, Object>> resultMap = new HashMap<>();

        if (!skipPredefinedSelection) {
            resultMap.put(RESULT, record);
        }

        doEnrichmentPostProcessing(record, forEnrichment, matchInput, resultMap);

        return resultMap;
    }

    private MatchOutput callMatch(MatchInput matchInput) {
        logInDebugMode("matchInput:", matchInput);

        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        logInDebugMode("matchOutput:", matchOutput);

        return matchOutput;
    }
}
