package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Seed class that is used internally. All {@link CDLMatchEntity} will be transformed into this class for lookup and
 * allocation so that common operations can be shared.
 * NOTE: The list of {@link CDLLookupEntry} passed in should ALREADY BE SORTED based on the priority (from high to low).
 * NOTE: Classes outside datacloud/match should use seed for specific entity instead. E.g., {@link CDLAccountSeed}.
 */
public class CDLRawSeed {
    private final String id; // entity ID
    private final CDLMatchEntity entity; // CDL entity
    private final List<CDLLookupEntry> lookupEntries; // list of lookup entry associated, sorted by entry priority, desc
    private final Map<String, String> attributes; // extra attributes to stored in the seed, cannot be used for lookup

    public CDLRawSeed(
            @NotNull String id, @NotNull CDLMatchEntity entity,
            @NotNull List<CDLLookupEntry> lookupEntries, Map<String, String> attributes) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(lookupEntries);
        this.id = id;
        this.entity = entity;
        // defensive copy, lookup key is immutable so no need to copy its fields
        this.lookupEntries = Collections.unmodifiableList(lookupEntries);
        this.attributes = attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(attributes);
    }

    public String getId() {
        return id;
    }

    public CDLMatchEntity getEntity() {
        return entity;
    }

    public List<CDLLookupEntry> getLookupEntries() {
        return lookupEntries;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    /*
     * Generated equals and hashCode
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CDLRawSeed rawSeed = (CDLRawSeed) o;
        return Objects.equal(id, rawSeed.id) &&
                Objects.equal(entity, rawSeed.entity) &&
                Objects.equal(lookupEntries, rawSeed.lookupEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, entity, lookupEntries);
    }
}
