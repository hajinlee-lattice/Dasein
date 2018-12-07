package com.latticeengines.datacloud.match.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;

/**
 * Internal service to manipulate {@link CDLLookupEntry} and {@link CDLRawSeed} for CDL entities. Data integrity
 * constraints will be preserved during these operations.
 */
public interface CDLEntityMatchInternalService {

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
     * list of seed IDs. If no seed mapped by specific lookup entry, {@literal null} will
     * be inserted in the respective index.
     */
    List<String> getIds(@NotNull Tenant tenant, @NotNull List<CDLLookupEntry> lookupEntries);

    /**
     * Retrieve {@link CDLRawSeed} with the given ID under the target tenant.
     *
     * @param tenant target tenant
     * @param entity target entity
     * @param seedId seed ID
     * @return seed object, {@literal null} if no seed with the specified ID exists
     */
    CDLRawSeed get(@NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull String seedId);

    /**
     * Retrieve a list of {@link CDLRawSeed} with a list of seed IDs.
     *
     * @param tenant target tenant
     * @param entity target entity
     * @param seedIds list of seed IDs
     * @return a list of seed IDs. the list will not be {@literal null} and will have the same size as the input
     * list of seed IDs. If no seed with a specific ID exists, {@literal null} will be inserted in the respective index.
     */
    List<CDLRawSeed> get(@NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull List<String> seedIds);

    // TODO consider to add get seed by lookup entry (not sure will be needed)

    /**
     * Allocate a new ID for given CDL entity in the specified tenant.
     *
     * @param tenant target tenant
     * @param entity target entity
     * @return the allocated ID, will not be {@literal null}
     */
    String allocateId(@NotNull Tenant tenant, @NotNull BusinessEntity entity);

    /**
     * Associate all lookup entries and attributes in the input {@link CDLRawSeed} to the current ones and return
     * all lookup entries that cannot be associated (have conflict with current entries).
     *
     * @param tenant target tenant
     * @param seed seed object containing lookup entries and attributes that we want to associate
     * @return a triple where
     *   the left object is the state before association
     *   the middle list contains all lookup entries that cannot be associated to the current seed.
     *   the right list contains all lookup entries that already mapped to another seed
     * @throws UnsupportedOperationException if allocating new accounts are not supported
     */
    Triple<CDLRawSeed, List<CDLLookupEntry>, List<CDLLookupEntry>> associate(
            @NotNull Tenant tenant, @NotNull CDLRawSeed seed);
}
