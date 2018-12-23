package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Request class for entity association.
 */
public class EntityAssociationRequest {
    /*
     * all fields must be already standardized
     */
    private final boolean allocateNewId;
    private final Tenant tenant;
    private final String entity;
    // user need to sort the lookup results based on the priority (DESC)
    private final List<Pair<MatchKeyTuple, String>> lookupResults;
    private final Map<String, String> extraAttributes;

    public EntityAssociationRequest(
            @NotNull Tenant tenant, @NotNull String entity, boolean allocateNewId,
            @NotNull List<Pair<MatchKeyTuple, String>> lookupResults, Map<String, String> extraAttributes) {
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(lookupResults);
        this.tenant = tenant;
        this.entity = entity;
        this.lookupResults = lookupResults;
        this.allocateNewId = allocateNewId;
        this.extraAttributes = extraAttributes == null ? Collections.emptyMap() : extraAttributes;
    }

    public boolean shouldAllocateNewId() {
        return allocateNewId;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public List<Pair<MatchKeyTuple, String>> getLookupResults() {
        return lookupResults;
    }

    public Map<String, String> getExtraAttributes() {
        return extraAttributes;
    }
}
