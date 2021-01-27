package com.latticeengines.datacloud.match.service.impl;

import java.time.Duration;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.dynamo.DynamoRetryPolicy;
import com.latticeengines.aws.dynamo.DynamoRetryUtils;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("entityMatchConfigurationService")
public class EntityMatchConfigurationServiceImpl implements EntityMatchConfigurationService {

    private static final Logger log = LoggerFactory.getLogger(EntityMatchConfigurationServiceImpl.class);

    /*
     * TODO put these in property file after cache setting is finalized
     */
    private static final long LOOKUP_CACHE_MAX_IDLE_SECONDS = 3600; // 1 hr
    private static final long LOOKUP_CACHE_MAX_MEMORY_MB = 1024; // 1G
    private static final long SEED_CACHE_MAX_IDLE_SECONDS = 3600; // 1 hr
    private static final long SEED_CACHE_MAX_MEMORY_MB = 2048; // 2G
    private static final long ANONYMOUS_SEED_CACHE_MAX_IDLE_SECONDS = 24 * 3600; // 1 day
    private static final long ANONYMOUS_SEED_CACHE_MAX_MEMORY_MB = 256; // 256M
    private static final long DEFAULT_MAX_WAIT_TIME = 30000L; // 30s

    @Value("${datacloud.match.entity.staging.shards:5}")
    private volatile int numStagingShards;
    @Value("${datacloud.match.entity.staging.table}")
    private volatile String stagingTableName;
    @Value("${datacloud.match.entity.serving.table}")
    private volatile String servingTableName;
    @Value("${datacloud.match.entity.staging.ttl:2629746}")
    private volatile long stagingTTLInSeconds; // expire 1 month
    @Value("${datacloud.match.entity.staging.copy.lazy:false}")
    private volatile boolean lazyCopyToStaging;
    /*
     * TODO tune these for staging/serving environment separately
     */
    @Value("${proxy.retry.initialwaitmsec:500}")
    private volatile long initialWaitMsec;
    @Value("${proxy.retry.multiplier:2}")
    private volatile double multiplier;
    @Value("${proxy.retry.maxattempts:5}")
    private volatile int maxAttempts;
    private volatile RetryTemplate retryTemplate;
    private volatile boolean isAllocateMode = false;
    private volatile Map<String, Boolean> perEntityAllocationModes;

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
    public void setNumShards(@NotNull EntityMatchEnvironment environment, int numShards) {
        Preconditions.checkArgument(EntityMatchEnvironment.STAGING.equals(environment));
        numStagingShards = numShards;
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
    public Duration getMaxAnonymousSeedCacheIdleDuration() {
        return Duration.ofSeconds(ANONYMOUS_SEED_CACHE_MAX_IDLE_SECONDS);
    }

    @Override
    public long getMaxAnonymousSeedCacheInMB() {
        return ANONYMOUS_SEED_CACHE_MAX_MEMORY_MB;
    }

    @Override
    public RetryTemplate getRetryTemplate(@NotNull EntityMatchEnvironment env) {
        // lazy instantiation
        if (retryTemplate == null) {
            synchronized (this) {
                if (retryTemplate == null) {
                    retryTemplate = DynamoRetryUtils.getExponentialBackOffTemplate(maxAttempts, initialWaitMsec,
                            DEFAULT_MAX_WAIT_TIME, multiplier);
                    retryTemplate.registerListener(getMetricsRetryListener(env));
                }
            }
        }
        return retryTemplate;
    }

    @Override
    public void setPerEntityAllocationModes(Map<String, Boolean> perEntityAllocationModes) {
        this.perEntityAllocationModes = perEntityAllocationModes;
    }

    @Override
    public void setIsAllocateMode(boolean isAllocateMode) {
        this.isAllocateMode = isAllocateMode;
    }

    @Override
    public boolean isAllocateMode(String entity) {
        if (StringUtils.isBlank(entity) || perEntityAllocationModes == null) {
            // no entity specified, return the global value
            return isAllocateMode;
        }
        // default to global value if not set
        return perEntityAllocationModes.getOrDefault(entity, isAllocateMode);
    }

    @Override
    public void setStagingTableName(String stagingTableName) {
        Preconditions.checkNotNull(stagingTableName, "staging table name should not be null");
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

    @Override
    public boolean shouldCopyToStagingLazily() {
        return lazyCopyToStaging;
    }

    @Override
    public void setShouldCopyToStagingLazily(boolean shouldCopyToStagingLazily) {
        lazyCopyToStaging = shouldCopyToStagingLazily;
    }

    @Override
    public EntityMatchConfiguration getConfiguration(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant, "tenant object should not be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(tenant.getId()), "Tenant ID should not be blank");
        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
            Path path = PathBuilder.buildMatchConfigurationPath(CamilleEnvironment.getPodId(), customerSpace);
            if (!camille.exists(path)) {
                return null;
            }

            String data = camille.get(path).getData();
            return JsonUtils.deserialize(data, EntityMatchConfiguration.class);
        } catch (Exception e) {
            log.error("Failed to retrieve match configuration for tenant " + tenant.getId(), e);
            return null;
        }
    }

    @Override
    public void saveConfiguration(@NotNull Tenant tenant, @NotNull EntityMatchConfiguration configuration) {
        Preconditions.checkNotNull(tenant, "tenant object should not be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(tenant.getId()), "Tenant ID should not be blank");
        Preconditions.checkNotNull(configuration, "Configuration object should not be null");

        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
            Path path = PathBuilder.buildMatchConfigurationPath(CamilleEnvironment.getPodId(), customerSpace);
            String data = JsonUtils.serialize(configuration);
            Document doc = new Document(data);
            camille.upsert(path, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            String msg = String.format("Failed to save match configuration (%s) for tenant %s",
                    JsonUtils.serialize(configuration), tenant);
            log.error(msg, e);
            throw new RuntimeException(e);
        }
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
                if (DynamoRetryPolicy.isThrottlingError(throwable)) {
                    entityMatchMetricService.recordDynamoThrottling(env, getTableName(env));
                } else {
                    log.error("Retrying count = {}, error = {}", context.getRetryCount(), throwable);
                }
                super.onError(context, callback, throwable);
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
                    Throwable throwable) {
                // NOTE retry template should only be used for dynamo call
                entityMatchMetricService.recordDynamoCall(env, getTableName(env), context,
                        DynamoRetryPolicy.isThrottlingError(throwable));
                super.close(context, callback, throwable);
            }
        };
    }
}
