package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.security.Tenant;

import java.util.List;

/**
 * Service to manipulate {@link CDLLookupEntry} in different environment (e.g., staging, serving) for the given tenant.
 * Note that lookup entries cannot be modified once created.
 */
public interface CDLLookupEntryService {

    /**
     * Retrieve the seed ID allocated to the given entity under the given tenant.
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param lookupEntry entry to lookup with
     * @return seed ID associated with the lookup, {@literal null} if no seed associated with the entry.
     */
    String get(@NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull CDLLookupEntry lookupEntry);

    /**
     * Retrieve a list of seed IDs allocated to the given entity under the given tenant.
     *
     * @param env environment to retrieve the entry from
     * @param tenant target tenant
     * @param lookupEntry list of entries to lookup with
     * @return a list of seed IDs associated with the lookup entries. the list will not be {@literal null} and will
     * be the same size as the input lookup entry list. if no seed associated with a lookup entry,
     * {@literal null} will be inserted in the respective index.
     */
    List<String> get(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant, @NotNull List<CDLLookupEntry> lookupEntry);

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
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull CDLLookupEntry lookupEntry, @NotNull String seedId);
}
