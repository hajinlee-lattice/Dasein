package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLAssociationResponse;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Associate all the lookup entry for a single record to one CDL entity.
 * The CDL entity to associate to is decide by the result of lookups. If no CDL entity found or there are conflicts
 * between the entity seed and lookup entries, a new entity will be created (and a new ID allocated).
 *
 * Input data: type={@link CDLAssociationRequest}
 * Output: {@link CDLAssociationResponse}
 */
@Component("cdlAssociateService")
public class CDLAssociateServiceImpl extends DataSourceLookupServiceBase {
    @Override
    protected CDLAssociationResponse lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        CDLAssociationRequest associationReq = (CDLAssociationRequest) request.getInputData();
        // TODO implement
        return null;
    }

    @Override
    protected void asyncLookupFromService(
            String lookupRequestId, DataSourceLookupRequest request, String returnAddress) {
        // TODO implement
        sendResponse(lookupRequestId, lookupFromService(lookupRequestId, request), returnAddress);
    }

    /*
     * FIXME remove this function once the real implementation is done
     * Fake the result for now to unblock other's work.
     *
     * 1. if any lookup result found an ID, return the highest priority one
     * 2. else if allocateNewId == true, return a newly allocated ID
     * 3. else, return null associated entity ID
     */
    private CDLAssociationResponse generateFakeResult(@NotNull CDLAssociationRequest request) {
        List<Pair<CDLLookupEntry, String>> lookupResults = request.getLookupResults();
        // sort base on priority
        lookupResults.sort((p1, p2) -> request.getLookupEntryComparator().compare(p1.getKey(), p2.getKey()));
        Pair<CDLLookupEntry, String> chosen = lookupResults
                .stream().filter(p -> p.getValue() != null).findFirst().orElseGet(() -> null);
        if (chosen != null) {
            // associate with current entity
            return new CDLAssociationResponse(request.getEntity(), chosen.getValue());
        } else {
            // allocate case
            String id = RandomStringUtils.randomAlphanumeric(16).toLowerCase();
            return new CDLAssociationResponse(request.getEntity(), request.shouldAllocateNewId() ? id : null);
        }
    }
}
