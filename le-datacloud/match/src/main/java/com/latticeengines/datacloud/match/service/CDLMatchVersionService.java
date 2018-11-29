package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Service to manage versioning for CDL match in different {@link CDLMatchEnvironment} and {@link Tenant}
 */
public interface CDLMatchVersionService {

    /**
     * Retrieve the current match version for given environment and tenant
     *
     * @param environment target environment
     * @param tenant target tenant, should have non-null {@link Tenant#getPid()} field
     * @return current match version
     */
    int getCurrentVersion(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant);

    /**
     * Increase the current match version for given environment and tenant by one
     *
     * @param environment target environment
     * @param tenant target tenant, should have non-null {@link Tenant#getPid()} field
     * @return match version after the increment
     */
    int bumpVersion(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant);

    /**
     * Clear the match version for given environment and tenant
     *
     * @param environment target environment
     * @param tenant target tenant, should have non-null {@link Tenant#getPid()} field
     */
    void clearVersion(@NotNull CDLMatchEnvironment environment, @NotNull Tenant tenant);
}
