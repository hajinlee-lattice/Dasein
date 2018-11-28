package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Seed class that is used internally. All {@link BusinessEntity} will be transformed into this class for lookup and
 * allocation so that common operations can be shared.
 * NOTE: The list of {@link CDLLookupEntry} passed in should ALREADY BE SORTED based on the priority (from high to low).
 * NOTE: Classes outside datacloud/match should use seed for specific entity instead. E.g., {@link CDLAccountSeed}.
 */
public class CDLRawSeed {
    private final String id; // entity ID
    private final BusinessEntity entity; // CDL entity
    private final int version; // internal version for optimistic locking
    private final List<CDLLookupEntry> lookupEntries; // list of lookup entry associated, sorted by entry priority, desc
    private final Map<String, String> attributes; // extra attributes to stored in the seed, cannot be used for lookup

    public CDLRawSeed(
            @NotNull String id, @NotNull BusinessEntity entity,
            @NotNull List<CDLLookupEntry> lookupEntries, Map<String, String> attributes) {
        this(id, entity, -1, lookupEntries, attributes);
    }

    public CDLRawSeed(
            @NotNull String id, @NotNull BusinessEntity entity, int version,
            @NotNull List<CDLLookupEntry> lookupEntries, Map<String, String> attributes) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(lookupEntries);
        this.id = id;
        this.entity = entity;
        this.version = version;
        // defensive copy, lookup key is immutable so no need to copy its fields
        this.lookupEntries = Collections.unmodifiableList(lookupEntries);
        this.attributes = attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(attributes);
    }

    public String getId() {
        return id;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public int getVersion() {
        return version;
    }

    public List<CDLLookupEntry> getLookupEntries() {
        return lookupEntries;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    /*
     * Generated equals and hashCode (version is internal and not used here)
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
