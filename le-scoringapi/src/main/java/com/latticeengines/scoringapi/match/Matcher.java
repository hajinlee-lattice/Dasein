package com.latticeengines.scoringapi.match;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.score.AdditionalScoreConfig;
import com.latticeengines.scoringapi.score.BulkMatchingContext;
import com.latticeengines.scoringapi.score.SingleMatchingContext;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

public interface Matcher {
    String RESULT = "RESULT";
    String ENRICHMENT = "ENRICHMENT";

    boolean accept(boolean isBulk);

    Map<String, Map<String, Object>> matchAndJoin(AdditionalScoreConfig additionalScoreConfig,
            SingleMatchingContext singleMatchingConfig, InterpretedFields interpreted, //
            Map<String, Object> record, //
            boolean forEnrichment);

    Map<RecordModelTuple, Map<String, Map<String, Object>>> matchAndJoin(//
            AdditionalScoreConfig additionalScoreConfig, BulkMatchingContext bulkMatchingConfig,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, boolean shouldEnrichOnly);

    List<LeadEnrichmentAttribute> getEnrichmentMetadata(CustomerSpace space,
            List<RecordModelTuple> partiallyOrderedParsedTupleList, boolean enrichInternalAttributes);
}
