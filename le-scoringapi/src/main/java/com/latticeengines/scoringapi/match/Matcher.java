package com.latticeengines.scoringapi.match;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

public interface Matcher {
    public static final String RESULT = "RESULT";
    public static final String ENRICHMENT = "ENRICHMENT";

    public boolean accept(boolean isBulk);

    Map<String, Map<String, Object>> matchAndJoin(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            boolean forEnrichment, //
            boolean enrichInternalAttributes, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode, //
            List<String> matchLogs, List<String> matchErrorLogs, //
            boolean shouldReturnAllEnrichment);

    Map<String, Map<String, Object>> matchAndJoin(CustomerSpace space, //
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
            boolean enforceFuzzyMatch, boolean skipDnBCache);

    Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(CustomerSpace space, //
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
            Map<RecordModelTuple, List<String>> matchErrorLogMap);

}
