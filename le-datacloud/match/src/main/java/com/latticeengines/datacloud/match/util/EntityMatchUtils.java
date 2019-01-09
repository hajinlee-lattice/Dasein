package com.latticeengines.datacloud.match.util;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;

import java.util.Objects;
import java.util.Optional;

/**
 * Utility functions for entity match
 */
public class EntityMatchUtils {

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
