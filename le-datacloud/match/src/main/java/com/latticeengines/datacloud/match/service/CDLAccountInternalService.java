package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

import java.util.List;

/**
 * Internal service to manipulate {@link CDLLookupEntry} and {@link CDLRawSeed} for account entity. Data integrity
 * constraints will be preserved during these operations.
 */
public interface CDLAccountInternalService {

    /**
     * Retrieve the seed ID with the input lookup entry.
     *
     * @param tenant target tenant
     * @param lookupEntry entry used to lookup the seed
     * @return seed ID mapped by the lookup entry, {@literal null} if no seed mapped by the entry
     */
    String getId(@NotNull Tenant tenant, @NotNull CDLLookupEntry lookupEntry);

    /**
     * Retrieve a list of seed IDs with the input list of lookup entries.
     *
     * @param tenant target tenant
     * @param lookupEntries a list of entries used to lookup seeds
     * @return a list of seed IDs. the list will not be {@literal null} and will have the same size as the input
     * list of seed IDs. If no seed with a specific ID exists, {@literal null} will be inserted in the respective index.
     */
    List<String> getIds(@NotNull Tenant tenant, @NotNull List<CDLLookupEntry> lookupEntries);

    /**
     * Retrieve {@link CDLRawSeed} with the given ID under the target tenant.
     *
     * @param tenant target tenant
     * @param seedId seed ID
     * @return seed object, {@literal null} if no seed with the specified ID exists
     */
    CDLRawSeed get(@NotNull Tenant tenant, @NotNull String seedId);

    // TODO consider to add bulk get seed (not sure will be needed)
    // TODO consider to add get seed by lookup entry (not sure will be needed)

    /**
     * Allocate CDL account ID in the specified tenant.
     *
     * @param tenant target tenant
     * @return the allocated ID, will not be {@literal null}
     */
    String allocateId(@NotNull Tenant tenant);

    /**
     * Associate a list of {@link CDLLookupEntry} to a CDL account seed.
     *
     * @param tenant target tenant
     * @param id CDL account ID to associate the entries to
     * @param entriesToAssociate list of lookup entries to be added to the specified seed
     * @param latticeAccountId lattice account ID to add to the seed, can be {@literal null}
     * @return seed object after the association finishes.
     */
    CDLRawSeed associate(
            @NotNull Tenant tenant, @NotNull String id,
            @NotNull List<CDLLookupEntry> entriesToAssociate, String latticeAccountId);
}
