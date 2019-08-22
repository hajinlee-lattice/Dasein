package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Commit entity match staging results to serving environment
 */
public interface EntityMatchCommitter {

    /**
     * Commit one entity from staging to serving
     *
     * @param entity
     *            target entity
     * @param tenant
     *            target tenant
     * @param destTTLEnabled
     *            use TTL or not in serving env. if {@literal null}, default value
     *            for serving environment will be used
     * @return non {@literal null} stats class
     */
    EntityPublishStatistics commit(@NotNull String entity, @NotNull Tenant tenant, Boolean destTTLEnabled);

    /**
     * Commit one entity from staging to serving with specific number of r/w workers
     *
     * @param entity
     *            target entity
     * @param tenant
     *            target tenant
     * @param destTTLEnabled
     *            use TTL or not in serving env. if {@literal null}, default value
     *            for serving environment will be used
     * @param nReader
     *            number of background readers
     * @param nWriter
     *            number of background writers
     * @return non {@literal null} stats class
     */
    EntityPublishStatistics commit(@NotNull String entity, @NotNull Tenant tenant, Boolean destTTLEnabled, int nReader,
            int nWriter);
}
