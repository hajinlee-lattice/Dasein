package com.latticeengines.datacloud.match.service;

import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
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
     * @param versionMap
     *            user specified match version for each
     *            {@link EntityMatchEnvironment}, current version will be used if no
     *            version is specified for certain environment
     * @return non {@code null} stats class
     */
    EntityPublishStatistics commit(@NotNull String entity, @NotNull Tenant tenant, Boolean destTTLEnabled,
            Map<EntityMatchEnvironment, Integer> versionMap);

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
     * @param versionMap
     *            user specified match version for each
     *            {@link EntityMatchEnvironment}, current version will be used if no
     *            version is specified for certain environment
     * @return non {@code null} stats class
     */
    EntityPublishStatistics commit(@NotNull String entity, @NotNull Tenant tenant, Boolean destTTLEnabled,
            Map<EntityMatchEnvironment, Integer> versionMap, int nReader,
            int nWriter);
}
