package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLEntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLAccountSeed;
import com.latticeengines.domain.exposed.security.Tenant;

import java.util.List;

/**
 * Service to retrieve {@link CDLAccountSeed}. This class can be used by external classes of datacloud/match. For
 * internal usage, see {@link CDLEntityMatchInternalService}
 */
public interface CDLAccountService {

    /**
     * Retrieve {@link CDLAccountSeed} with the given ID under the target tenant.
     *
     * @param tenant target tenant
     * @param cdlAccountId target ID
     * @return seed object, {@literal null} if no seed with the specified ID exists
     */
    CDLAccountSeed get(@NotNull Tenant tenant, @NotNull String cdlAccountId);

    /**
     *
     *
     * @param tenant target tenant
     * @param cdlAccountIds input list of IDs
     * @return a list of seed objects. the list will not be {@literal null} and will have the same size as the input
     * list of IDs. If no seed with a specific ID exists, {@literal null} will be inserted in the respective index.
     */
    List<CDLAccountSeed> get(@NotNull Tenant tenant, @NotNull List<String> cdlAccountIds);
}
