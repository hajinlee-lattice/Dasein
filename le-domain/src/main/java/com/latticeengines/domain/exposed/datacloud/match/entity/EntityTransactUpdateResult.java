package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Result of merging {@link EntityRawSeed} and set up lookup mapping for
 * {@link EntityLookupEntry} using transaction
 */
public class EntityTransactUpdateResult {
    private final boolean succeeded;
    private final EntityRawSeed seed; // seed after merging, nullable
    private final Map<EntityLookupEntry, String> entriesMapToOtherSeeds;

    public EntityTransactUpdateResult(boolean succeeded, EntityRawSeed seed,
            Map<EntityLookupEntry, String> entriesMapToOtherSeeds) {
        this.succeeded = succeeded;
        this.seed = seed;
        this.entriesMapToOtherSeeds = entriesMapToOtherSeeds == null ? Collections.emptyMap()
                : ImmutableMap.copyOf(entriesMapToOtherSeeds);
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public EntityRawSeed getSeed() {
        return seed;
    }

    public Map<EntityLookupEntry, String> getEntriesMapToOtherSeeds() {
        return entriesMapToOtherSeeds;
    }

    @Override
    public String toString() {
        return "EntityTransactUpdateResult{" + "succeeded=" + succeeded + ", seed=" + seed + ", entriesMapToOtherSeeds="
                + entriesMapToOtherSeeds + '}';
    }
}
