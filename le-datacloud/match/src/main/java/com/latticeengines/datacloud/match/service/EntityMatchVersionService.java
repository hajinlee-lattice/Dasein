package com.latticeengines.datacloud.match.service;

import java.util.ConcurrentModificationException;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Service to manage versioning for entity match in different {@link EntityMatchEnvironment} and {@link Tenant}
 */
public interface EntityMatchVersionService {

    /**
     * Retrieve the current match version for given environment and tenant
     *
     * @param environment target environment
     * @param tenant target tenant, should have non-null {@link Tenant#getPid()} field
     * @return current match version
     */
    int getCurrentVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant);

    /**
     * Return the version that will be applied after next
     * {@link EntityMatchVersionService#bumpVersion(EntityMatchEnvironment, Tenant)}
     * call.
     *
     * @param environment
     *            target environment
     * @param tenant
     *            target tenant, should have non-null {@link Tenant#getPid()} field
     * @return next version after increment
     */
    int getNextVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant);

    /**
     * Increase the current match version for given environment and tenant.
     *
     * @param environment
     *            target environment
     * @param tenant
     *            target tenant, should have non-null {@link Tenant#getPid()} field
     * @throws ConcurrentModificationException
     *             multiple clients modifying version at the same time
     * @return match version after the increment
     */
    int bumpVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant);

    /**
     * Set current version to a desired value. The target version should be one of
     * the value returned by
     * {@link EntityMatchVersionService#getCurrentVersion(EntityMatchEnvironment, Tenant)}
     * before.
     *
     * @param environment
     *            target environment
     * @param tenant
     *            target tenant, should have non-null {@link Tenant#getPid()} field
     * @param version
     *            desired version to set current version to
     * @throws IllegalArgumentException
     *             if target version is not valid
     * @throws ConcurrentModificationException
     *             multiple clients modifying version at the same time
     */
    void setVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant, int version);

    /**
     * Clear the match version for given environment and tenant
     *
     * @param environment target environment
     * @param tenant target tenant, should have non-null {@link Tenant#getPid()} field
     */
    void clearVersion(@NotNull EntityMatchEnvironment environment, @NotNull Tenant tenant);
}
