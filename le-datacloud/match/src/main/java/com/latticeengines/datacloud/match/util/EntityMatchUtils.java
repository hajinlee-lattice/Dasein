package com.latticeengines.datacloud.match.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ENTITY_PREFIX_SEED_ATTRIBUTES;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Utility functions for entity match
 */
public class EntityMatchUtils {

    // Match Target Entity -> Set(Entity that we will output a list of newly
    // allocated IDs)
    private static final Map<String, Set<String>> OUTPUT_NEW_ENTITY_MAP = new HashMap<>();
    // Match Target Entity -> Set(Attribute names that will only be set when this
    // attribute does not exist in seed)
    private static final Map<String, Set<String>> FIRST_WIN_ATTRIBUTES = new HashMap<>();

    static {
        OUTPUT_NEW_ENTITY_MAP.put(Contact.name(), Sets.newHashSet(Account.name()));
        // currently only LDC ID in account match is first win, others are last win
        FIRST_WIN_ATTRIBUTES.put(Account.name(), Collections.singleton(InterfaceName.LatticeAccountId.name()));
    }

    /**
     * Generate a (somewhat) user friendly string for the input
     * {@link EntityLookupEntry}
     *
     * @param entry
     *            target entry
     * @return string representation of the entry
     */
    public static String prettyToString(EntityLookupEntry entry) {
        if (entry == null) {
            return StringUtils.EMPTY;
        }

        String[] values;
        switch (entry.getType()) {
        case DUNS:
            return String.format("{DUNS=%s}", entry.getSerializedValues());
        case NAME_COUNTRY:
            values = entry.getValues();
            return String.format("{Name=%s,Country=%s}", values[0], values[1]);
        case DOMAIN_COUNTRY:
            values = entry.getValues();
            return String.format("{Domain=%s,Country=%s}", values[0], values[1]);
        case EXTERNAL_SYSTEM:
            return String.format("{System=%s,ID=%s}", entry.getSerializedKeys(), entry.getSerializedValues());
        }
        return StringUtils.EMPTY;
    }

    /**
     * Check if input entry has conflict with target seed (same serialized key, different value). Only evaluate entries
     * with X to one mapping.
     *
     * @param seed target seed
     * @param entry entry to be evaluated
     * @return true if the input entry has conflict with target seed
     */
    public static boolean hasConflictInSeed(@NotNull EntityRawSeed seed, @NotNull EntityLookupEntry entry) {
        Preconditions.checkNotNull(seed);
        Preconditions.checkNotNull(entry);
        if (entry.getType().mapping == EntityLookupEntry.Mapping.MANY_TO_MANY) {
            // only consider x to one
            return false;
        }

        Optional<EntityLookupEntry> conflictEntry = seed.getLookupEntries().stream()
                .filter(entryInSeed -> entryInSeed.getType() == entry.getType())
                .filter(entryInSeed -> Objects.equals(entryInSeed.getSerializedKeys(), entry.getSerializedKeys()))
                .filter(entryInSeed -> !Objects.equals(entryInSeed.getSerializedValues(), entry.getSerializedValues()))
                .findAny();
        return conflictEntry.isPresent();
    }

    /**
     * Create a new {@link Tenant} with ID fields standardized
     * 
     * @param tenant
     *            input tenant, must not be {@literal null} and contains non-null ID
     * @return non null tenant with standardized ID field
     */
    public static Tenant newStandardizedTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getId());
        return new Tenant(CustomerSpace.parse(tenant.getId()).getTenantId());
    }

    /**
     * Based on the input environment, determine whether we should set TTL for seed/lookup entries or not.
     *
     * @param env input environment
     * @return true if we should set TTL
     */
    public static boolean shouldSetTTL(EntityMatchEnvironment env) {
        // at the moment, only set TTL for staging environment
        return env == EntityMatchEnvironment.STAGING;
    }

    /**
     * Determine whether it needs to output a list of newly allocated entities.
     *
     * @param input
     *            target match input
     * @return true if needed to output, false otherwise
     */
    public static boolean shouldOutputNewEntities(MatchInput input) {
        return isAllocateIdModeEntityMatch(input) && input.isOutputNewEntities()
                && OUTPUT_NEW_ENTITY_MAP.containsKey(input.getTargetEntity());
    }

    /**
     * Determine whether we should output current entity given the original target
     * entity in {@link MatchInput}
     *
     * @param input
     *            original match input instance
     * @param currentEntity
     *            entity for current decision graph
     * @return true if we want to output newly allocated current entity, false
     *         otherwise.
     */
    public static boolean shouldOutputNewEntity(MatchInput input, String currentEntity) {
        return shouldOutputNewEntities(input) && OUTPUT_NEW_ENTITY_MAP.containsKey(input.getTargetEntity())
                && OUTPUT_NEW_ENTITY_MAP.get(input.getTargetEntity()).contains(currentEntity);
    }

    /**
     * Helper to determine from current {@link MatchInput} whether it is entity
     * match and is in allocateId mode.
     *
     * @param input
     *            target match input object
     * @return true if it is entity match and is in allocate mode
     */
    public static boolean isAllocateIdModeEntityMatch(MatchInput input) {
        if (input == null) {
            return false;
        }

        return OperationalMode.ENTITY_MATCH.equals(input.getOperationalMode()) && input.isAllocateId();
    }

    /**
     * Determine whether the target tuple has only System IDs match key combination.
     * I.e., only {@link EntityLookupEntry.Type#EXTERNAL_SYSTEM} will be able to
     * transform the tuple to {@link EntityLookupEntry}.
     *
     * @param tuple
     *            target tuple
     * @return true if only systemIds
     */
    public static boolean hasSystemIdsOnly(MatchKeyTuple tuple) {
        if (tuple == null) {
            return false;
        }

        return isNotEmpty(tuple.getSystemIds()) && isBlank(tuple.getEmail()) && isBlank(tuple.getName())
                && isBlank(tuple.getPhoneNumber());
    }

    /**
     * Determine whether email field is mapped in current match and is the only
     * field used as domain in account match.
     *
     * @param traveler
     *            current traveler object
     * @return true if no other account info than Email
     */
    public static boolean hasEmailAccountInfoOnly(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        MatchKeyTuple accountTuple = traveler.getEntityMatchKeyTuple(BusinessEntity.Account.name());
        if (tuple == null) {
            // be definsive for now, these two should not be null, maybe fail later
            return false;
        }

        // Assumption is: If Contact has Email, it must be mapped in
        // Account Domain match key because in Account match, there is
        // no concept of "Email", thus we can only detect how many
        // domain fields are mapped
        return tuple.getEmail() != null
                && (getValidEntityId(BusinessEntity.Account.name(), traveler) == null || (accountTuple != null
                        && accountTuple.hasDomainOnly() && !accountTuple.isDomainFromMultiCandidates()));
    }

    /**
     * Determine whether we should override target attribute value or only set if
     * the attribute does not exist
     *
     * @param entity
     *            target entity in current match
     * @param attrName
     *            attribute name
     * @return true if we should override, false if we should set only when not
     *         exist
     */
    public static boolean shouldOverrideAttribute(@NotNull String entity, @NotNull String attrName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity),
                String.format("Target entity=%s should not be blank", entity));
        Preconditions.checkArgument(StringUtils.isNotBlank(attrName),
                String.format("Attribute name [%s] should not be blank", attrName));
        if (attrName.startsWith(ENTITY_PREFIX_SEED_ATTRIBUTES)) {
            attrName = attrName.substring(ENTITY_PREFIX_SEED_ATTRIBUTES.length());
        }

        // attrs not in firstWinMap are considered last win (override with new value)
        return !FIRST_WIN_ATTRIBUTES.getOrDefault(entity, Collections.emptySet()).contains(attrName);
    }

    /**
     * Return valid matched entity ID (ID of a non-anonymous entity) of specified
     * entity
     *
     * @param entity
     *            target entity
     * @param traveler
     *            traveler instance
     * @return valid entity ID, {@literal null} if no valid ID found
     */
    public static String getValidEntityId(@NotNull String entity, MatchTraveler traveler) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity),
                String.format("Target entity=%s should not be blank", entity));
        if (traveler == null || MapUtils.isEmpty(traveler.getEntityIds())) {
            return null;
        }

        String entityId = traveler.getEntityIds().get(entity);
        if (StringUtils.isBlank(entityId) || DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(entityId)) {
            return null;
        }
        return entityId;
    }

    /**
     * Get match key -> column name map for target entity in the given input.
     *
     * @param input
     *            match input object
     * @param entity
     *            target entity
     * @return map for target entity, return an empty map if no such map exists
     */
    public static Map<MatchKey, List<String>> getKeyMapForEntity(MatchInput input, @NotNull String entity) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity),
                String.format("Target entity=%s should not be blank", entity));
        if (input == null || MapUtils.isEmpty(input.getEntityKeyMaps())) {
            return Collections.emptyMap();
        }
        if (input.getEntityKeyMaps().get(entity) == null) {
            return Collections.emptyMap();
        }

        Map<MatchKey, List<String>> keyMap = input.getEntityKeyMaps().get(entity).getKeyMap();
        return keyMap == null ? new HashMap<>() : keyMap;
    }
}
