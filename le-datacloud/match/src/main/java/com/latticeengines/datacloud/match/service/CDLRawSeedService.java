package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import java.util.List;

/**
 * Service to manipulate {@link CDLRawSeed} in different environment (e.g., staging, serving) for the given tenant.
 */
public interface CDLRawSeedService {

    /**
     * Create an empty seed with the specified info. Only create if a seed with the same info does NOT already exist.
     *
     * @param env environment to create the entry in
     * @param tenant target tenant
     * @param entity target entity type
     * @param seedId seed ID
     * @return true if the seed is created
     */
    boolean createIfNotExists(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId);

    /**
     * Set the seed entry with the specified info. Only set if a seed with the same info does NOT already exist.
     *
     * @param env environment to create the entry in
     * @param tenant target tenant
     * @param seed seed object to be set
     * @return true if the seed is set
     */
    boolean setIfNotExists(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed seed);

    /**
     * Retrieve the seed with the specified into.
     *
     * @param env environment to retrieve from
     * @param tenant target tenant
     * @param entity target entity type
     * @param seedId seed ID
     * @return retrieved seed object, {@literal null} if not exist
     */
    CDLRawSeed get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId);

    /**
     * Retrieve a list of seeds with the specified into.
     *
     * @param env environment to retrieve from
     * @param tenant target tenant
     * @param entity target entity type
     * @param seedIds list of seed IDs to retrieve
     * @return a list of seed objects. the list will not be {@literal null} and will have the same size as the input
     * list of seed IDs. If no seed with a specific ID exists, {@literal null} will be inserted in the respective index.
     */
    List<CDLRawSeed> get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull List<String> seedIds);

    /**
     * Update all {@link com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry} and attributes in the
     * seed. Seed will be created if it does not already exist.
     * 1. For lookup entry type that has X to one mapping to the seed, only update if the seed does NOT already
     *    have entry with the same type/key.
     * 2. For attributes, later one will override the previous one.
     *
     * @param env environment to perform operation in
     * @param tenant target tenant
     * @param rawSeed seed object used to udpate
     * @return seed before update (can know whether a lookup key entry is updated successfully by looking at the value)
     */
    CDLRawSeed updateIfNotSet(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed rawSeed);

    /**
     * Clear all {@link com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry} and attributes if the
     * seed has the same version as the one specified in the input.
     *
     * @param env environment to perform operation in
     * @param tenant target tenant
     * @param rawSeed seed object used to clear
     * @return cleared seed, {@literal null} if no such seed exists
     * @throws IllegalStateException if the seed has a different version as the one specified in the input
     */
    CDLRawSeed clearIfEquals(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed rawSeed);

    /**
     * Clear all {@link com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry} and attributes if the
     * seed has the same version as the one specified in the input.
     *
     * @param env environment to perform operation in
     * @param tenant target tenant
     * @param rawSeed seed object used to clear
     * @return cleared seed, {@literal null} if no such seed exists
     */
    CDLRawSeed clear(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLRawSeed rawSeed);

    /**
     * Delete the seed with the specified info.
     *
     * @param env environment to perform operation in
     * @param tenant target tenant
     * @param entity target entity
     * @param seedId seed ID
     * @return true if the seed exists and is deleted
     */
    boolean delete(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId);
}
