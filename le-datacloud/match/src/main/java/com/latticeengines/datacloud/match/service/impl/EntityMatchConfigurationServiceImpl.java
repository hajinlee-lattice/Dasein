package com.latticeengines.datacloud.match.service.impl;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;

@Component("entityMatchConfigurationService")
public class EntityMatchConfigurationServiceImpl implements EntityMatchConfigurationService {

    /*
     * TODO put these in property file after cache setting is finalized
     */
    private static final long LOOKUP_CACHE_MAX_IDLE_SECONDS = 3600; // 1 hr
    private static final long LOOKUP_CACHE_MAX_MEMORY_MB = 1024; // 1G
    private static final long SEED_CACHE_MAX_IDLE_SECONDS = 3600; // 1 hr
    private static final long SEED_CACHE_MAX_MEMORY_MB = 2048; // 2G

    private static final Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTIONS = new HashMap<>();

    static {
        // instantiate retry exception map

        // exceptions that can be retried
        RETRY_EXCEPTIONS.put(LimitExceededException.class, true);
        RETRY_EXCEPTIONS.put(ProvisionedThroughputExceededException.class, true);
        RETRY_EXCEPTIONS.put(ItemCollectionSizeLimitExceededException.class, true);
        RETRY_EXCEPTIONS.put(InternalServerErrorException.class, true);
        // exceptions that cannot be retried
        RETRY_EXCEPTIONS.put(ResourceInUseException.class, false);
        RETRY_EXCEPTIONS.put(ResourceNotFoundException.class, false);
        RETRY_EXCEPTIONS.put(ConditionalCheckFailedException.class, false);
    }

    @Value("${datacloud.match.entity.staging.shards:5}")
    private int numStagingShards;
    @Value("${datacloud.match.entity.staging.table}")
    private String stagingTableName;
    @Value("${datacloud.match.entity.serving.table}")
    private String servingTableName;
    @Value("${datacloud.match.entity.staging.ttl:2629746}")
    private long stagingTTLInSeconds; // expire 1 month
    /*
     * TODO tune these for staging/serving environment separately
     */
    @Value("${proxy.retry.initialwaitmsec:500}")
    private long initialWaitMsec;
    @Value("${proxy.retry.multiplier:2}")
    private double multiplier;
    @Value("${proxy.retry.maxattempts:5}")
    private int maxAttempts;
    private volatile RetryTemplate retryTemplate;
    private volatile boolean isAllocateMode = false;

    @Lazy
    @Inject
    private EntityMatchMetricService entityMatchMetricService;

    @Override
    public String getTableName(@NotNull EntityMatchEnvironment environment) {
        Preconditions.checkNotNull(environment);
        switch (environment) {
            case SERVING:
                return servingTableName;
            case STAGING:
                return stagingTableName;
            default:
                throw new UnsupportedOperationException("Unsupported environment: " + environment);
        }
    }

    @Override
    public int getNumShards(@NotNull EntityMatchEnvironment environment) {
        Preconditions.checkArgument(EntityMatchEnvironment.STAGING.equals(environment));
        // currently only staging environment needs sharding
        return numStagingShards;
    }

    @Override
    public long getExpiredAt() {
        return getExpiredAt(System.currentTimeMillis() / 1000);
    }

    @Override
    public long getExpiredAt(long timestampInSeconds) {
        return timestampInSeconds + stagingTTLInSeconds;
    }

    @Override
    public Duration getMaxLookupCacheIdleDuration() {
        return Duration.ofSeconds(LOOKUP_CACHE_MAX_IDLE_SECONDS);
    }

    @Override
    public long getMaxLookupCacheMemoryInMB() {
        return LOOKUP_CACHE_MAX_MEMORY_MB;
    }

    @Override
    public Duration getMaxSeedCacheIdleDuration() {
        return Duration.ofSeconds(SEED_CACHE_MAX_IDLE_SECONDS);
    }

    @Override
    public long getMaxSeedCacheMemoryInMB() {
        return SEED_CACHE_MAX_MEMORY_MB;
    }

    @Override
    public RetryTemplate getRetryTemplate(@NotNull EntityMatchEnvironment env) {
        // lazy instantiation
        if (retryTemplate == null) {
            synchronized (this) {
                if (retryTemplate == null) {
                    retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate(
                            maxAttempts, initialWaitMsec, multiplier, RETRY_EXCEPTIONS);
                    retryTemplate.registerListener(getMetricsRetryListener(env));
                }
            }
        }
        return retryTemplate;
    }

    @Override
    public void setIsAllocateMode(boolean isAllocateMode) {
        this.isAllocateMode = isAllocateMode;
    }

    @Override
    public boolean isAllocateMode() {
        return isAllocateMode;
    }

    @VisibleForTesting
    public void setNumStagingShards(int numStagingShards) {
        this.numStagingShards = numStagingShards;
    }

    @VisibleForTesting
    public void setStagingTableName(String stagingTableName) {
        this.stagingTableName = stagingTableName;
    }

    @VisibleForTesting
    public void setServingTableName(String servingTableName) {
        this.servingTableName = servingTableName;
    }

    @VisibleForTesting
    public void setStagingTTLInSeconds(long stagingTTLInSeconds) {
        this.stagingTTLInSeconds = stagingTTLInSeconds;
    }

    @VisibleForTesting
    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    /*
     * Listener to record dynamo read/write throttling metrics
     */
    private RetryListener getMetricsRetryListener(@NotNull EntityMatchEnvironment env) {
        return new RetryListenerSupport() {
            // TODO somehow differentiate between read/write dynamo request
            @Override
            public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
                    Throwable throwable) {
                if (isThrottleError(throwable)) {
                    entityMatchMetricService.recordDynamoThrottling(env, getTableName(env));
                }
                super.onError(context, callback, throwable);
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
                    Throwable throwable) {
                // NOTE retry template should only be used for dynamo call
                entityMatchMetricService.recordDynamoCall(env, getTableName(env), context, isThrottleError(throwable));
                super.close(context, callback, throwable);
            }

            private boolean isThrottleError(Throwable throwable) {
                return ExceptionUtils.indexOfType(throwable, ProvisionedThroughputExceededException.class) != -1;
            }
        };
    }
}
