package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Request class for entity association.
 */
public class EntityAssociationRequest {
    private final Tenant tenant;
    private final String entity;
    private final Map<EntityMatchEnvironment, Integer> versionMap;
    private final String preferredEntityId; // preferred allocate ID for this request
    // user need to sort the lookup results based on the priority (DESC)
    // The following preconditions must be met, this makes implementation of association simpler
    // NOTE one MatchKeyTuple should only contain one lookup entry (i.e., for systemIds, caller need to split
    //      a list of systemIds into multiple lists that have 1 systemId)
    // NOTE Calling EntityLookupEntryConverter.fromMatchKeyTuple on any tuple should get a list of ONE lookup entry
    private final List<Pair<MatchKeyTuple, String>> lookupResults;
    private final Map<String, String> extraAttributes;
    private final List<Integer> dummyLookupResultIndices; // idx of artificially created lookup results
    // whether to use txn implementation (less chance of splitting records,
    // trade-off
    // on performance)
    private final boolean useTransactAssociate;

    public EntityAssociationRequest(
            @NotNull Tenant tenant, @NotNull String entity, Map<EntityMatchEnvironment, Integer> versionMap,
            String preferredEntityId,
            @NotNull List<Pair<MatchKeyTuple, String>> lookupResults, Map<String, String> extraAttributes) {
        this(tenant, entity, versionMap, preferredEntityId, lookupResults, extraAttributes, null, false);
    }

    public EntityAssociationRequest(@NotNull Tenant tenant, @NotNull String entity,
            Map<EntityMatchEnvironment, Integer> versionMap,
            String preferredEntityId, @NotNull List<Pair<MatchKeyTuple, String>> lookupResults,
            Map<String, String> extraAttributes, List<Integer> dummyLookupResultIndices, boolean useTransactAssociate) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(lookupResults);
        this.tenant = tenant;
        this.entity = entity;
        this.versionMap = versionMap;
        this.preferredEntityId = preferredEntityId;
        this.lookupResults = lookupResults;
        this.extraAttributes = extraAttributes == null ? Collections.emptyMap() : extraAttributes;
        this.dummyLookupResultIndices = dummyLookupResultIndices == null ? Collections.emptyList()
                : dummyLookupResultIndices;
        this.useTransactAssociate = useTransactAssociate;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public Map<EntityMatchEnvironment, Integer> getVersionMap() {
        return versionMap;
    }

    public String getPreferredEntityId() {
        return preferredEntityId;
    }

    public List<Pair<MatchKeyTuple, String>> getLookupResults() {
        return lookupResults;
    }

    public Map<String, String> getExtraAttributes() {
        return extraAttributes;
    }

    public List<Integer> getDummyLookupResultIndices() {
        return dummyLookupResultIndices;
    }

    public boolean isUseTransactAssociate() {
        return useTransactAssociate;
    }
}
