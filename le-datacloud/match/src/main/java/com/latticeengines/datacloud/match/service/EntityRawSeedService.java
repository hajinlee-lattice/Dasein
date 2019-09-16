package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Service to manipulate {@link EntityRawSeed} in different environment (e.g., staging, serving) for the given tenant.
 */
public interface EntityRawSeedService {

    /**
     * Create an empty seed with the specified info. Only create if a seed with the
     * same info does NOT already exist.
     *
     * @param env
     *            environment to create the entry in
     * @param tenant
     *            target tenant
     * @param entity
     *            target entity type
     * @param seedId
     *            seed ID
     * @param setTTL
     *            whether we should set TTL
     * @param version
     *            specific version used for this operation
     * @return true if the seed is created
     */
    boolean createIfNotExists(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId, boolean setTTL, int version);

    /**
     * Set the seed entry with the specified info. Only set if a seed with the same
     * info does NOT already exist.
     *
     * @param env
     *            environment to create the entry in
     * @param tenant
     *            target tenant
     * @param seed
     *            seed object to be set
     * @param setTTL
     *            whether we should set TTL
     * @param version
     *            specific version used for this operation
     * @return true if the seed is set
     */
    boolean setIfNotExists(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityRawSeed seed, boolean setTTL,
            int version);

    /**
     * Retrieve the seed with the specified into.
     *
     * @param env
     *            environment to retrieve from
     * @param tenant
     *            target tenant
     * @param entity
     *            target entity type
     * @param seedId
     *            seed ID
     * @param version
     *            specific version used for this operation
     * @return retrieved seed object, {@literal null} if not exist
     */
    EntityRawSeed get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId, int version);

    /**
     * Retrieve a list of seeds with the specified into.
     *
     * @param env
     *            environment to retrieve from
     * @param tenant
     *            target tenant
     * @param entity
     *            target entity type
     * @param seedIds
     *            list of seed IDs to retrieve
     * @param version
     *            specific version used for this operation
     * @return a list of seed objects. the list will not be {@literal null} and will
     *         have the same size as the input list of seed IDs. If no seed with a
     *         specific ID exists, {@literal null} will be inserted in the
     *         respective index.
     */
    List<EntityRawSeed> get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull List<String> seedIds, int version);

    /**
     * Scan dynamo and return a set of seeds with the specified into.
     *
     * @param env
     *            environment to retrieve from(only support STAGING right now)
     * @param tenant
     *            target tenant
     * @param entity
     *            target entity type
     * @param seedIds
     *            list of seed IDs used as start keys, null/empty indicates all
     *            shards.
     * @param version
     *            specific version used for this operation
     * @return seed objects map. the map will be {@literal null} if scan to the end,
     *         map key is shard id, value is list of seed IDs scanned in that
     *         shards.
     */
    Map<Integer, List<EntityRawSeed>> scan(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, List<String> seedIds, int maxResultSize, int version);

    /**
     * Update all {@link EntityLookupEntry} and attributes in the seed. Seed will be
     * created if it does not already exist. 1. For lookup entry type that has X to
     * one mapping to the seed, only update if the seed does NOT already have entry
     * with the same type/key. 2. For attributes, later one will override the
     * previous one.
     *
     * @param env
     *            environment to perform operation in
     * @param tenant
     *            target tenant
     * @param rawSeed
     *            seed object used to udpate
     * @param setTTL
     *            whether we should set TTL
     * @param version
     *            specific version used for this operation
     * @return seed before update (can know whether a lookup key entry is updated
     *         successfully by looking at the value)
     */
    EntityRawSeed updateIfNotSet(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityRawSeed rawSeed, boolean setTTL, int version);

    /**
     * Clear all {@link EntityLookupEntry} and attributes if the seed has the same
     * version as the one specified in the input.
     *
     * @param env
     *            environment to perform operation in
     * @param tenant
     *            target tenant
     * @param rawSeed
     *            seed object used to clear
     * @param version
     *            specific version used for this operation
     * @return cleared seed, {@literal null} if no such seed exists
     * @throws IllegalStateException
     *             if the seed has a different version as the one specified in the
     *             input
     */
    EntityRawSeed clearIfEquals(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityRawSeed rawSeed, int version);

    /**
     * Clear all {@link EntityLookupEntry} and attributes if the seed has the same
     * version as the one specified in the input.
     *
     * @param env
     *            environment to perform operation in
     * @param tenant
     *            target tenant
     * @param rawSeed
     *            seed object used to clear
     * @param version
     *            specific version used for this operation
     * @return cleared seed, {@literal null} if no such seed exists
     */
    EntityRawSeed clear(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityRawSeed rawSeed, int version);

    /**
     * Delete the seed with the specified info.
     *
     * @param env
     *            environment to perform operation in
     * @param tenant
     *            target tenant
     * @param entity
     *            target entity
     * @param seedId
     *            seed ID
     * @param version
     *            specific version used for this operation
     * @return true if the seed exists and is deleted
     */
    boolean delete(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull String entity, @NotNull String seedId, int version);

    /**
     * Batch create seeds.
     *
     * @param env
     *            environment to perform operation in
     * @param tenant
     *            target tenant
     * @param rawSeeds
     *            seed list to be created.
     * @param setTTL
     *            whether we should set TTL
     * @param version
     *            specific version used for this operation
     * @return true if the batch create succeeded
     */
    boolean batchCreate(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, List<EntityRawSeed> rawSeeds, boolean setTTL,
            int version);
}
