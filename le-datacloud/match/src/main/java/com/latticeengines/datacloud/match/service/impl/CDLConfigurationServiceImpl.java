package com.latticeengines.datacloud.match.service.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLConfigurationService;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("cdlMatchConfiguration")
public class CDLConfigurationServiceImpl implements CDLConfigurationService {

    @Value("${datacloud.match.cdl.staging.shards:5}")
    private int numStagingShards;
    @Value("${datacloud.match.cdl.staging.table}")
    private String stagingTableName;
    @Value("${datacloud.match.cdl.serving.table}")
    private String servingTableName;
    @Value("${datacloud.match.cdl.staging.ttl:2629746}")
    private long stagingTTLInSeconds; // expire 1 month

    @Override
    public String getTableName(@NotNull CDLMatchEnvironment environment) {
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
    public int getNumShards(@NotNull CDLMatchEnvironment environment) {
        Preconditions.checkArgument(CDLMatchEnvironment.STAGING.equals(environment));
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
}
