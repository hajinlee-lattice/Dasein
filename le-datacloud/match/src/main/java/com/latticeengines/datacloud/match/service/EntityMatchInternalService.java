package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Triple;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Internal service to manipulate {@link EntityLookupEntry} and {@link EntityRawSeed} for entities. Data integrity
 * constraints will be preserved during these operations.
 */
public interface EntityMatchInternalService {

    /**
     * Retrieve the seed ID with the input lookup entry.
     *
     * @param tenant target tenant
     * @param lookupEntry entry used to lookup the seed
     * @return seed ID mapped by the lookup entry, {@literal null} if no seed mapped by the entry
     */
    String getId(@NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry);

    /**
     * Retrieve a list of seed IDs with the input list of lookup entries.
     *
     * @param tenant target tenant
     * @param lookupEntries a list of entries used to lookup seeds
     * @return a list of seed IDs. the list will not be {@literal null} and will have the same size as the input
     * list of seed IDs. If no seed mapped by specific lookup entry, {@literal null} will
     * be inserted in the respective index.
     */
    List<String> getIds(@NotNull Tenant tenant, @NotNull List<EntityLookupEntry> lookupEntries);

    /**
     * Retrieve {@link EntityRawSeed} with the given ID under the target tenant.
     *
     * @param tenant target tenant
     * @param entity target entity
     * @param seedId seed ID
     * @return seed object, {@literal null} if no seed with the specified ID exists
     */
    EntityRawSeed get(@NotNull Tenant tenant, @NotNull String entity, @NotNull String seedId);

    /**
     * Retrieve a list of {@link EntityRawSeed} with a list of seed IDs.
     *
     * @param tenant target tenant
     * @param entity target entity
     * @param seedIds list of seed IDs
     * @return a list of seed IDs. the list will not be {@literal null} and will have the same size as the input
     * list of seed IDs. If no seed with a specific ID exists, {@literal null} will be inserted in the respective index.
     */
    List<EntityRawSeed> get(@NotNull Tenant tenant, @NotNull String entity, @NotNull List<String> seedIds);

    /**
     * Retrieve anonymoous seed for target tenant/entity. Create if it does not
     * already exist. isNewlyAllocated flag will be set accordingly.
     *
     * @param tenant
     *            target tenant
     * @param entity
     *            target entity
     * @return anonymous seed object, will not be {@literal null}
     */
    EntityRawSeed getOrCreateAnonymousSeed(@NotNull Tenant tenant, @NotNull String entity);

    /**
     * Allocate a new ID for given entity in the specified tenant.
     *
     * @param tenant target tenant
     * @param entity target entity
     * @return the allocated ID, will not be {@literal null}
     */
    String allocateId(@NotNull Tenant tenant, @NotNull String entity);

    /**
     * Associate all lookup entries and attributes in the input
     * {@link EntityRawSeed} to the current ones and return all lookup entries that
     * cannot be associated (have conflict with current entries).
     *
     * @param tenant
     *            target tenant
     * @param seed
     *            seed object containing lookup entries and attributes that we want
     *            to associate
     * @param clearAllFailedLookupEntries
     *            true if we clear all lookup entries that failed to set lookup
     *            mapping, false if we only clear one to one entries that failed
     * @param entriesMapToOtherSeed
     *            set of entries that are already map to other seeds, can be
     *            {@literal null}
     * @return a triple where the left object is the state before association the
     *         middle list contains all lookup entries that cannot be associated to
     *         the current seed. the right list contains all lookup entries that
     *         already mapped to another seed
     * @throws UnsupportedOperationException
     *             if allocating new accounts are not supported
     */
    Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> associate(
            @NotNull Tenant tenant, @NotNull EntityRawSeed seed, boolean clearAllFailedLookupEntries,
            Set<EntityLookupEntry> entriesMapToOtherSeed);

    /**
     * Cleanup entity seed that is supposed to be orphan (not mapped by any of its
     * lookup entry)
     * 
     * @param tenant
     *            target tenant
     * @param entity
     *            entity
     * @param seedId
     *            seed ID to cleanup
     */
    void cleanupOrphanSeed(@NotNull Tenant tenant, @NotNull String entity, @NotNull String seedId);

    /**
     * Publish seed/lookup data from source tenant (staging env) to dest tenant
     * (staging/serving env)
     *
     * Current use case:
     *
     * STAGING -> SERVING env with same tenant in PA publish
     * STAGING -> STAGING env with different tenant in checkpoint save/restore
     *
     * @param entity
     * @param sourceTenant
     * @param destTenant
     * @param destEnv
     * @param destTTLEnabled:
     *            If null, by default, true if destEnv is STAGING and false if
     *            destEnv is SERVING
     */
    EntityPublishStatistics publishEntity(@NotNull String entity, @NotNull Tenant sourceTenant,
            @NotNull Tenant destTenant,
            @NotNull EntityMatchEnvironment destEnv, Boolean destTTLEnabled);
}
