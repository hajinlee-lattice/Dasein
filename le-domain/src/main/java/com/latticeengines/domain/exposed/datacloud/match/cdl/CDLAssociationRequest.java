package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Comparator;
import java.util.List;

/**
 * Request class for CDL entity association.
 */
public class CDLAssociationRequest {
    private final boolean allocateNewId;
    private final BusinessEntity entity;
    private final List<Pair<CDLLookupEntry, String>> lookupResults;
    // used to define the priority of lookup keys, from high to low priority (DESC).
    private final Comparator<CDLLookupEntry> lookupEntryComparator;

    public CDLAssociationRequest(
            @NotNull BusinessEntity entity, @NotNull List<Pair<CDLLookupEntry, String>> lookupResults,
            @NotNull Comparator<CDLLookupEntry> lookupEntryComparator, boolean allocateNewId) {
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(lookupResults);
        Preconditions.checkNotNull(lookupEntryComparator);
        this.entity = entity;
        this.lookupResults = lookupResults;
        this.lookupEntryComparator = lookupEntryComparator;
        this.allocateNewId = allocateNewId;
    }

    public boolean shouldAllocateNewId() {
        return allocateNewId;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public List<Pair<CDLLookupEntry, String>> getLookupResults() {
        return lookupResults;
    }

    public Comparator<CDLLookupEntry> getLookupEntryComparator() {
        return lookupEntryComparator;
    }
}
