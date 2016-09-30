package com.latticeengines.scoringapi.match.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.match.MatchInputBuilder;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

@Component
public class SingleRecordMatcher extends AbstractMatcher {
    protected static final Log log = LogFactory.getLog(SingleRecordMatcher.class);

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
        boolean shouldCallEnrichmentExplicitly = //
                shouldCallEnrichmentExplicitly(modelSummary, forEnrichment);

        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = //
                getEnrichmentMetadata(space);

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
            Map<String, Map<String, Object>> enrichmentResult = //
                    buildAndExecuteMatch(space, interpreted, //
                            fieldSchemas, record, //
                            null, true, //
                            selectedLeadEnrichmentAttributes, true, //
                            MatchTypeUtil.getVersionForEnforcingAccountMasterBasedMatch());

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

        MatchOutput matchOutput = null;
        if (shouldShortcircuitPropdata) {
            matchOutput = getRealTimeMatchService(//
                    matchInput.getDataCloudVersion()).match(matchInput);
        } else {
            matchOutput = matchProxy.matchRealTime(matchInput);
        }

        logInDebugMode("matchOutput:", matchOutput);

        return matchOutput;
    }

    private MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            String overrideDataCloudVersion) {
        MatchInputBuilder matchInputBuilder = //
                getMatchInputBuilder(getDataCloudVersion(modelSummary));
        return matchInputBuilder.buildMatchInput(space, interpreted, //
                record, modelSummary, //
                selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, //
                overrideDataCloudVersion);
    }

    private void doEnrichmentPostProcessing(Map<String, Object> record, //
            boolean forEnrichment, MatchInput matchInput, //
            Map<String, Map<String, Object>> resultMap) {
        if (forEnrichment) {
            Map<String, Object> enrichmentData = new HashMap<>();

            List<Column> customSelectionColumns = getCustomSelectionColumns(matchInput);

            if (customSelectionColumns != null) {
                for (Column attr : customSelectionColumns) {
                    if (record.containsKey(attr.getExternalColumnId())) {
                        enrichmentData.put(attr.getExternalColumnId(), //
                                record.get(attr.getExternalColumnId()));
                    }
                }
            }
            resultMap.put(ENRICHMENT, enrichmentData);
        }
    }

    private List<Column> getCustomSelectionColumns(MatchInput matchInput) {
        List<Column> customSelectionColumns = null;

        if (matchInput.getUnionSelection() != null //
                && matchInput.getUnionSelection().getCustomSelection() != null) {
            customSelectionColumns = matchInput.getUnionSelection().getCustomSelection().getColumns();
        } else if (matchInput.getCustomSelection() != null) {
            customSelectionColumns = matchInput.getCustomSelection().getColumns();
        }

        return customSelectionColumns;
    }
}
