package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import org.springframework.retry.support.RetryTemplate;

/**
 * Central place to manage all entity match related configurations
 */
public interface EntityMatchConfigurationService {

    /**
     * Get the table name for a given environment.
     *
     * @param environment target environment
     * @return table name, will not be {@literal null}
     * @throws UnsupportedOperationException if no table for given environment.
     */
    String getTableName(@NotNull EntityMatchEnvironment environment);

    /**
     * Retrieve the number of shards for a given environment
     *
     * @param environment target environment
     * @return number of shrads for given environment, will be a positive number
     * @throws UnsupportedOperationException if sharding is not supported for this environment
     */
    int getNumShards(@NotNull EntityMatchEnvironment environment);

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

    /**
     * Return retry template that is configured for the given environment. Same environment will always get the same
     * instance of retry template.
     *
     * @param env target environment
     * @return non-null retry template
     * @throws UnsupportedOperationException if the environment is not supported
     */
    RetryTemplate getRetryTemplate(@NotNull EntityMatchEnvironment env);
}
