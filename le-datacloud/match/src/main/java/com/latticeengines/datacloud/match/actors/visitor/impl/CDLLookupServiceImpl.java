package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.service.CDLEntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupRequest;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

/**
 * Lookup CDL entity ID with given {@link com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple}.
 * Handle request batching and async lookup at this service.
 *
 * Input data: type={@link EntityLookupRequest}
 * Output: type={@link com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupResponse}
 */
@Component("cdlLookupService")
public class CDLLookupServiceImpl extends DataSourceLookupServiceBase {

    @Inject
    private CDLEntityMatchInternalService cdlEntityMatchInternalService;

    @Override
    protected Pair<CDLLookupEntry, String> lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        EntityLookupRequest lookupRequest = (EntityLookupRequest) request.getInputData();
        String entity = lookupRequest.getEntity();
        MatchKeyTuple matchKeyTuple = lookupRequest.getTuple();
        // TODO generate response
        return generateFakeResult(matchKeyTuple);
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
     * The fake result is generated base on the following rules:
     * 1. if DUNS exists in input => generate DUNS lookup entry with ID
     * 2. else if Domain+Country exists => generate Domain+Country lookup entry with ID
     * 3. else if Name+Country exists => generate Name+Country lookup entry with ID
     * 4. else => generate external system lookup entry WITHOUT ID
     */
    private Pair<CDLLookupEntry, String> generateFakeResult(@NotNull MatchKeyTuple tuple) {
        Preconditions.checkNotNull(tuple);
        // not very random
        String id = RandomStringUtils.randomAlphanumeric(16).toLowerCase();

        if (StringUtils.isNotBlank(tuple.getDuns())) {
            return Pair.of(CDLLookupEntryConverter.fromDuns(BusinessEntity.Account.name(), tuple.getDuns()), id);
        } else if (StringUtils.isNotBlank(tuple.getDomain()) && StringUtils.isNotBlank(tuple.getCountry())) {
            return Pair.of(CDLLookupEntryConverter.fromDomainCountry(
                    BusinessEntity.Account.name(), tuple.getDomain(), tuple.getCountry()), id);
        } else if (StringUtils.isNotBlank(tuple.getName()) && StringUtils.isNotBlank(tuple.getCountry())) {
            return Pair.of(CDLLookupEntryConverter.fromNameCountry(
                    BusinessEntity.Account.name(), tuple.getName(), tuple.getCountry()), id);
        }
        // not found
        return Pair.of(CDLLookupEntryConverter.fromExternalSystem(
                BusinessEntity.Account.name(), "SFDC", "sfdc_id_1"), null);
    }
}
