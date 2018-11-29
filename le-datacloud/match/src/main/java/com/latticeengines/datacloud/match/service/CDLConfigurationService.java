package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;

/**
 * Central place to manage all CDL related configurations
 */
public interface CDLConfigurationService {

    /**
     * Get the table name for a given environment.
     *
     * @param environment target environment
     * @return table name, will not be {@literal null}
     * @throws UnsupportedOperationException if no table for given environment.
     */
    String getTableName(@NotNull CDLMatchEnvironment environment);

    /**
     * Retrieve the number of shards for a given environment
     *
     * @param environment target environment
     * @return number of shrads for given environment, will be a positive number
     * @throws UnsupportedOperationException if sharding is not supported for this environment
     */
    int getNumShards(@NotNull CDLMatchEnvironment environment);

    /**
     * Get the expired timestamp, starting from current time.
     *
     * @return expired timestamp (epoch) in seconds
     */
    long getExpiredAt();

    /**
     * Get the expired timestamp, starting from input timestamp.
     *
     * @param timestampInSeconds starting time
     * @return expired timestamp (epoch) in seconds
     */
    long getExpiredAt(long timestampInSeconds);

    // TODO add retry configuration methods
}
