package com.latticeengines.datacloud.match.util;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Utility functions for entity match
 */
public class EntityMatchUtils {

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
}
