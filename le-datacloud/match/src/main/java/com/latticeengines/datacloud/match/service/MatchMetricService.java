package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.actors.visitor.impl.DataSourceLookupServiceBase;
import com.latticeengines.datacloud.match.metric.DnBMatchHistory;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;

/**
 * Service for recording general match metrics
 */
public interface MatchMetricService {

    /**
     * Register {@link DataSourceLookupServiceBase} for monitoring saturation metrics
     *
     * @param service target service
     * @param isBatchMode flag for batch mode
     */
    void registerDataSourceLookupService(DataSourceLookupServiceBase service, boolean isBatchMode);

    /**
     * Record the size of batched requests that are being processed
     *
     * @param serviceName processing service name
     * @param isBatchMode flag for batch mode
     * @param size number of requests being processed
     */
    void recordBatchRequestSize(String serviceName, boolean isBatchMode, int size);

    /**
     * Record visit to individual actor
     *
     * @param history
     *            visit info
     */
    void recordActorVisit(VisitingHistory history);

    /**
     * Record DnB match result
     *
     * @param history
     *            DnB match result
     */
    void recordDnBMatch(DnBMatchHistory history);

    /**
     * Record result of a single match operation
     *
     * @param context
     *            ctx object containing input/output/stats of a single match
     */
    void recordMatchFinished(MatchContext context);

    /**
     * Record result of realtime bulk match (multiple realtime match request batched
     * together)
     *
     * @param bulkMatchOutput
     *            realtime bulk match output
     */
    void recordBulkSize(BulkMatchOutput bulkMatchOutput);

    /**
     * Record result of fuzzy match result of a single record
     *
     * @param history
     *            match result of one record
     */
    void recordFuzzyMatchRecord(FuzzyMatchHistory history);

}
