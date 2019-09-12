package com.latticeengines.datacloud.match.testframework;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDomainCountry;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromDuns;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter.fromNameCountry;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.service.impl.EntityMatchInternalServiceImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Service for entity match testing
 */
@Component("testEntityMatchService")
public class TestEntityMatchService {

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityMatchInternalServiceImpl entityMatchInternalService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    /**
     * Prepare {@link EntityLookupEntry} test data in specified environment using
     * raw object array.
     *
     * @param tenant
     *            target tenant
     * @param env
     *            target environment
     * @param entity
     *            entity name
     * @param header
     *            header for columns, {@link String} for systemId,
     *            {@link InterfaceName} for entityId and latticeAccountId,
     *            {@link MatchKey} for remaining match keys (domain, country, etc.)
     * @param lookupData
     *            raw data array (array of rows)
     * @param setTTL
     *            whether to expire test data in the specified environment
     * @return created test data, lookup entry -> entityId pairs
     */
    List<Pair<EntityLookupEntry, String>> prepareLookupTestData(@NotNull Tenant tenant,
            @NotNull EntityMatchEnvironment env, @NotNull String entity, @NotNull Object[] header,
            @NotNull Object[][] lookupData, boolean setTTL) {
        return prepareTestData(tenant, env, entity, header, lookupData, (req) -> {
            // create lookup entries
            List<Pair<EntityLookupEntry, String>> entries = getLookupEntryPairs(entity, req.matchKeyIdx,
                    req.entityIdIdx, req.systemIdx, lookupData);
            entityLookupEntryService.set(env, tenant, entries, setTTL);
            return entries;
        }, setTTL);
    }

    /**
     * Prepare {@link EntityRawSeed} test data in specified environment using raw
     * object array.
     *
     * @param tenant
     *            target tenant
     * @param env
     *            target environment
     * @param entity
     *            entity name
     * @param header
     *            header for columns, {@link String} for systemId,
     *            {@link InterfaceName} for entityId and latticeAccountId,
     *            {@link MatchKey} for remaining match keys (domain, country, etc.)
     * @param seedData
     *            raw data array (array of rows)
     * @param setTTL
     *            whether to expire test data in the specified environment
     * @return created test data
     */
    List<EntityRawSeed> prepareSeedTestData(@NotNull Tenant tenant, @NotNull EntityMatchEnvironment env,
            @NotNull String entity, @NotNull Object[] header, @NotNull Object[][] seedData, boolean setTTL) {
        return prepareTestData(tenant, env, entity, header, seedData, (req) -> {
            // create seeds
            List<EntityRawSeed> seeds = getSeeds(entity, req.matchKeyIdx, req.entityIdIdx, req.latticeAccountIdIdx,
                    req.systemIdx, seedData);
            boolean created = entityRawSeedService.batchCreate(env, tenant, seeds, setTTL,
                    entityMatchVersionService.getCurrentVersion(env, tenant));
            Preconditions.checkArgument(created);
            return seeds;
        }, setTTL);
    }

    /**
     * Bump the version all environments of given tenant
     *
     * @param tenantId
     *            target tenant ID
     */
    public void bumpVersion(@NotNull String tenantId) {
        Arrays.stream(EntityMatchEnvironment.values()).forEach(env -> bumpVersion(tenantId, env));

        // cleanup seed & lookup cache otherwise invalid entries with old version will
        // still be looked up
        invalidateSeedLookupCache();
    }

    /**
     * Bump the version for given environment and tenant
     *
     * @param tenantId
     *            target tenant ID
     * @param env
     *            target environment
     */
    public void bumpVersion(@NotNull String tenantId, @NotNull EntityMatchEnvironment env) {
        Preconditions.checkNotNull(tenantId);

        // standardize tenant ID
        Tenant tenant = new Tenant(CustomerSpace.parse(tenantId).getTenantId());
        entityMatchVersionService.bumpVersion(env, tenant);

        // cleanup seed & lookup cache otherwise invalid entries with old version will
        // still be looked up
        invalidateSeedLookupCache();
    }

    /*
     * cleanup lookup & seed cache if they are already instantiated
     */
    private void invalidateSeedLookupCache() {
        Cache<Pair<Pair<String, String>, String>, EntityRawSeed> seedCache = entityMatchInternalService.getSeedCache();
        if (seedCache != null) {
            seedCache.invalidateAll();
        }
        Cache<Pair<String, EntityLookupEntry>, String> lookupCache = entityMatchInternalService.getLookupCache();
        if (lookupCache != null) {
            lookupCache.invalidateAll();
        }
    }

    private <R> R prepareTestData(@NotNull Tenant tenant, @NotNull EntityMatchEnvironment env, @NotNull String entity,
            @NotNull Object[] header, @NotNull Object[][] data, Function<PrepareTestDataRequest, R> prepare,
            boolean setTTL) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getId());
        Preconditions.checkNotNull(env);
        Preconditions.checkNotNull(entity);
        checkTestDataHeader(header);
        checkData(header, data);

        // retrieve indexes from header
        int entityIdIdx = getEntityIdIdx(header);
        Map<MatchKey, Integer> matchKeyIdx = getMatchKeyIdxMap(header);
        Map<String, Integer> systemIdx = getSystemIdxMap(header);
        int latticeAccountIdIdx = getLatticeAccountIdx(header);

        return prepare.apply(new PrepareTestDataRequest(tenant, env, entity, entityIdIdx, latticeAccountIdIdx,
                matchKeyIdx, systemIdx, setTTL));
    }

    private List<EntityRawSeed> getSeeds(@NotNull String entity, @NotNull Map<MatchKey, Integer> matchKeyIdx,
            int entityIdIdx, int latticeAccountIdIdx, @NotNull Map<String, Integer> systemIdIdx,
            @NotNull Object[][] seedData) {
        Map<String, Set<EntityLookupEntry>> lookupEntryMap = Arrays.stream(seedData) //
                .map(row -> {
                    String entityId = (String) row[entityIdIdx];
                    Preconditions.checkNotNull(entityId);
                    Set<EntityLookupEntry> entries = new HashSet<>(
                            getLookupEntries(entity, matchKeyIdx, systemIdIdx, row));
                    return Pair.of(entityId, entries);
                }) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }));
        Map<String, String> latticeAccountIdMap = Arrays.stream(seedData) //
                .map(row -> {
                    String entityId = (String) row[entityIdIdx];
                    Preconditions.checkNotNull(entityId);
                    if (latticeAccountIdIdx >= 0) {
                        return Pair.of(entityId, (String) row[latticeAccountIdIdx]);
                    } else {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
        return Arrays.stream(seedData) //
                .map(row -> {
                    String entityId = (String) row[entityIdIdx];
                    Preconditions.checkNotNull(entityId);
                    return entityId;
                }) //
                .distinct() //
                .map(entityId -> {
                    List<EntityLookupEntry> entries = new ArrayList<>(
                            lookupEntryMap.getOrDefault(entityId, new HashSet<>()));
                    Map<String, String> extraAttributes = new HashMap<>();
                    if (latticeAccountIdMap.containsKey(entityId)) {
                        extraAttributes.put(DataCloudConstants.LATTICE_ACCOUNT_ID, latticeAccountIdMap.get(entityId));
                    }
                    return new EntityRawSeed(entityId, entity, entries, extraAttributes);
                }) //
                .collect(Collectors.toList());
    }

    private List<Pair<EntityLookupEntry, String>> getLookupEntryPairs(@NotNull String entity,
            @NotNull Map<MatchKey, Integer> matchKeyIdx, int entityIdIdx, @NotNull Map<String, Integer> systemIdIdx,
            @NotNull Object[][] lookupData) {
        return Arrays //
                .stream(lookupData) //
                .flatMap(row -> {
                    String entityId = (String) row[entityIdIdx];
                    Preconditions.checkNotNull(entityId);
                    return getLookupEntries(entity, matchKeyIdx, systemIdIdx, row).stream()
                            .map(entry -> Pair.of(entry, entityId));
                }) //
                .collect(Collectors.toList());
    }

    private List<EntityLookupEntry> getLookupEntries(@NotNull String entity,
            @NotNull Map<MatchKey, Integer> matchKeyIdx, @NotNull Map<String, Integer> systemIdIdx,
            @NotNull Object[] row) {
        String domain = getMatchValue(matchKeyIdx, MatchKey.Domain, row);
        String country = getMatchValue(matchKeyIdx, MatchKey.Country, row);
        String name = getMatchValue(matchKeyIdx, MatchKey.Name, row);
        String duns = getMatchValue(matchKeyIdx, MatchKey.DUNS, row);
        List<EntityLookupEntry> entries = new ArrayList<>();
        if (isNotBlank(domain) && isNotBlank(country)) {
            entries.add(fromDomainCountry(entity, domain, country));
        }
        if (isNotBlank(name) && isNotBlank(country)) {
            entries.add(fromNameCountry(entity, name, country));
        }
        if (isNotBlank(duns)) {
            entries.add(fromDuns(entity, duns));
        }
        systemIdIdx.forEach((system, idx) -> {
            String systemId = (String) row[idx];
            if (StringUtils.isBlank(systemId)) {
                return;
            }
            entries.add(EntityLookupEntryConverter.fromExternalSystem(entity, system, systemId));
        });
        return entries;
    }

    private String getMatchValue(Map<MatchKey, Integer> idxMap, MatchKey matchKey, Object[] row) {
        if (!idxMap.containsKey(matchKey)) {
            return null;
        }

        return (String) row[idxMap.get(matchKey)];
    }

    private Map<String, Integer> getSystemIdxMap(@NotNull Object[] header) {
        return IntStream.range(0, header.length) //
                .filter(idx -> header[idx] instanceof String) //
                .mapToObj(idx -> Pair.of((String) header[idx], idx)) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private Map<MatchKey, Integer> getMatchKeyIdxMap(@NotNull Object[] header) {
        return IntStream.range(0, header.length) //
                .filter(idx -> header[idx] instanceof MatchKey) //
                .mapToObj(idx -> Pair.of((MatchKey) header[idx], idx)) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private int getEntityIdIdx(@NotNull Object[] header) {
        List<Integer> entityIdIdxs = IntStream.range(0, header.length) //
                .filter(idx -> header[idx] == InterfaceName.EntityId) //
                .boxed() //
                .collect(Collectors.toList());
        Preconditions.checkArgument(entityIdIdxs.size() == 1);
        return entityIdIdxs.get(0);
    }

    private int getLatticeAccountIdx(@NotNull Object[] header) {
        List<Integer> latticeAccountIdxs = IntStream.range(0, header.length) //
                .filter(idx -> header[idx] == InterfaceName.LatticeAccountId) //
                .boxed() //
                .collect(Collectors.toList());
        Preconditions.checkArgument(latticeAccountIdxs.size() <= 1);
        return latticeAccountIdxs.isEmpty() ? -1 : latticeAccountIdxs.get(0);
    }

    private void checkData(@NotNull Object[] header, Object[][] data) {
        Preconditions.checkNotNull(data);
        Arrays.stream(data) //
                .forEach(row -> Preconditions.checkArgument(row != null && row.length == header.length));
    }

    /*
     * make sure the header only contains valid types
     */
    private void checkTestDataHeader(@NotNull Object[] header) {
        Preconditions.checkNotNull(header);
        Arrays.stream(header).forEach(val -> {
            Preconditions.checkNotNull(val);
            Preconditions.checkArgument(val instanceof MatchKey || val instanceof String
                    || val == InterfaceName.EntityId || val == InterfaceName.LatticeAccountId);
        });
    }

    private class PrepareTestDataRequest {
        final Tenant tenant;
        final EntityMatchEnvironment env;
        final String entity;
        final int entityIdIdx;
        final int latticeAccountIdIdx;
        final Map<MatchKey, Integer> matchKeyIdx;
        final Map<String, Integer> systemIdx;
        final boolean setTTL;

        PrepareTestDataRequest(Tenant tenant, EntityMatchEnvironment env, String entity, int entityIdIdx,
                int latticeAccountIdIdx, Map<MatchKey, Integer> matchKeyIdx, Map<String, Integer> systemIdx,
                boolean setTTL) {
            this.tenant = tenant;
            this.env = env;
            this.entity = entity;
            this.entityIdIdx = entityIdIdx;
            this.latticeAccountIdIdx = latticeAccountIdIdx;
            this.matchKeyIdx = matchKeyIdx;
            this.systemIdx = systemIdx;
            this.setTTL = setTTL;
        }
    }
}
