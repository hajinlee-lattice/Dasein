package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Service to manipulate {@link EntityLookupEntry} in different environment (e.g., staging, serving) for the given tenant.
 * Note that lookup entries cannot be modified once created.
 */
public interface EntityLookupEntryService {

    /**
     * Retrieve the seed ID allocated to the given entity under the given tenant.
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param lookupEntry entry to lookup with
     * @return seed ID associated with the lookup, {@literal null} if no seed associated with the entry.
     */
    String get(@NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry);

    /**
     * Retrieve a list of seed IDs allocated to the given entity under the given tenant.
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param lookupEntries list of entries to lookup with
     * @return a list of seed IDs associated with the lookup entries. the list will not be {@literal null} and will
     * be the same size as the input lookup entry list. if no seed associated with a lookup entry,
     * {@literal null} will be inserted in the respective index.
     */
    List<String> get(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull List<EntityLookupEntry> lookupEntries);

    /**
     * Create the mapping from the input lookup entry to the target seed ID. Only create if the input lookup entry
     * has NOT already mapped to a seed.
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param lookupEntry entry to create the mapping from
     * @param seedId seed ID to create the mapping to
     * @return true if mapping is created
     */
    boolean createIfNotExists(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityLookupEntry lookupEntry, @NotNull String seedId);

    /**
     * Set the mapping from the input lookup entry to the target seed ID. Only set if the input lookup entry
     * has NOT already mapped to a seed or has the same seed ID.
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param lookupEntry entry to create the mapping from
     * @param seedId seed ID to create the mapping to
     * @return true if mapping is set
     */
    boolean setIfEquals(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull EntityLookupEntry lookupEntry, @NotNull String seedId);

    /**
     * Set the list of mapping from the input [ lookup entry, seedId ] pair. If there are duplicate lookup entry,
     * the result will be undefined (one of the seed ID will be set).
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param pairs list of lookup entry / seed ID pair that will be set
     */
    void set(@NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, List<Pair<EntityLookupEntry, String>> pairs);

    /**
     * Delete the specified lookup entry.
     *
     * @param env environment to perform operation in
     * @param tenant target tenant
     * @param lookupEntry target lookup entry
     * @return true if the entry exists and is deleted, false otherwise
     */
    boolean delete(@NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry);
}
