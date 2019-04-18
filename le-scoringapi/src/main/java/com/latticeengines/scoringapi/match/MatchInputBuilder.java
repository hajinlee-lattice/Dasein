package com.latticeengines.scoringapi.match;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

public interface MatchInputBuilder {
    boolean accept(String dataCloudVersion);

    MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode);

    MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            String overrideDataCloudVersion, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode);

    MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, //
            Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            String overrideDataCloudVersion, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode, //
            boolean enforceFuzzyMatch, boolean skipDnBCache);

    BulkMatchInput buildMatchInput(CustomerSpace space, //
            List<RecordModelTuple> partiallyOrderedParsedTupleList, //
            List<ModelSummary> originalOrderModelSummaryList, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean isHomogeneous, //
            boolean skipPredefinedSelection, //
            boolean performFetchOnlyForMatching, //
            String requestId, boolean isDebugMode);
}
