package com.latticeengines.datacloud.match.util;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Utility functions for entity match
 */
public class EntityMatchUtils {

    // Match Target Entity -> Set(Entity that we will output a list of newly
    // allocated IDs)
    private static final Map<String, Set<String>> OUTPUT_NEW_ENTITY_MAP = new HashMap<>();

    static {
        // FIXME put account here for testing, no need to output new accounts when it is
        // Account match. remove this after we have contact match
        OUTPUT_NEW_ENTITY_MAP.put(Account.name(), Sets.newHashSet(Account.name()));
        OUTPUT_NEW_ENTITY_MAP.put(Contact.name(), Sets.newHashSet(Account.name()));
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
}
