package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.actors.visitor.impl.DataSourceLookupServiceBase;

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
}
