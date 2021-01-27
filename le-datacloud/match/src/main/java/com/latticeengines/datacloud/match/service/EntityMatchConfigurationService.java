package com.latticeengines.datacloud.match.service;

import java.time.Duration;
import java.util.Map;

import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

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
     * @param environment
     *            target environment
     * @return number of shards for given environment, will be a positive number
     * @throws UnsupportedOperationException
     *             if sharding is not supported for this environment
     */
    int getNumShards(@NotNull EntityMatchEnvironment environment);

    /**
     * Overwrite number of shards for given environment
     *
     * @param environment
     *            target environment
     * @param numShards
     *            value to overwrite
     */
    void setNumShards(@NotNull EntityMatchEnvironment environment, int numShards);

    /**
     * Overwrite table used as staging environment
     *
     * @param stagingTableName
     *            table name
     */
    void setStagingTableName(@NotNull String stagingTableName);

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
     * Return the duration that an lookup cache entry is allowed to stay in cache without being accessed
     *
     * @return non-null duration object
     */
    Duration getMaxLookupCacheIdleDuration();

    /**
     * Return the maximum memory allowed to use for lookup entry's in memory cache
     *
     * @return non-negative number (in megabytes)
     */
    long getMaxLookupCacheMemoryInMB();

    /**
     * Return the duration that an seed cache entry is allowed to stay in cache without being accessed
     *
     * @return non-null duration object
     */
    Duration getMaxSeedCacheIdleDuration();

    /**
     * Return the maximum memory allowed to use for seed's in memory cache
     *
     * @return non-negative number (in megabytes)
     */
    long getMaxSeedCacheMemoryInMB();

    /**
     * Return the duration that an anonymous seed cache entry is allowed to stay in
     * cache without being accessed
     *
     * @return non-null duration object
     */
    Duration getMaxAnonymousSeedCacheIdleDuration();

    /**
     * Return the maximum memory allowed to use for anonymous seed's in memory cache
     *
     * @return non-negative number (in megabytes)
     */
    long getMaxAnonymousSeedCacheInMB();

    /**
     * Return retry template that is configured for the given environment. Same environment will always get the same
     * instance of retry template.
     *
     * @param env target environment
     * @return non-null retry template
     * @throws UnsupportedOperationException if the environment is not supported
     */
    RetryTemplate getRetryTemplate(@NotNull EntityMatchEnvironment env);

    /**
     * Configure whether to allocate/create specific entity. This map will overwrite
     * the global flag. E.g., Account -> true, Contact -> false means we will only
     * match to existing contact but not creating new ones
     *
     * @param perEntityAllocationModes
     *            map of entity -> allocateId flag (create new entity if true)
     */
    void setPerEntityAllocationModes(Map<String, Boolean> perEntityAllocationModes);

    /**
     * Setter for isAllocateMode flag. See {@link this#isAllocateMode(String)} for the flag's meaning.
     *
     * @param isAllocateMode input flag to be set
     */
    void setIsAllocateMode(boolean isAllocateMode);

    /**
     * Return a flag to represent whether system is in allocate mode. True means we
     * will allocate new entity ID and merge to existing entity. False means read
     * only mode and will return no match in case of conflict.
     *
     * @param entity
     *            current entity that's being matched
     * @return current value of the flag
     */
    boolean isAllocateMode(String entity);

    /**
     * Return a flag to indicate whether match should lazily copy seed/lookup from
     * serving to staging
     *
     * @return current value of the flag
     */
    boolean shouldCopyToStagingLazily();

    /**
     * Configure whether match should lazily copy seed/lookup from serving to
     * staging
     *
     * @param shouldCopyToStagingLazily
     *            true to lazily copy
     */
    void setShouldCopyToStagingLazily(boolean shouldCopyToStagingLazily);

    /**
     * Retrieve persisted configuration for target tenant.
     *
     * @param tenant
     * @return config object, {@code null} if not configuration exists
     */
    EntityMatchConfiguration getConfiguration(@NotNull Tenant tenant);

    /**
     * Persist given configuration for target tenant
     *
     * @param tenant
     *            tenant object
     * @param configuration
     *            config object
     */
    void saveConfiguration(@NotNull Tenant tenant, @NotNull EntityMatchConfiguration configuration);
}
