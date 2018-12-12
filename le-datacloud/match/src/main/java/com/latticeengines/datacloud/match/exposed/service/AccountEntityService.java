package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.entity.AccountSeed;
import com.latticeengines.domain.exposed.security.Tenant;

import java.util.List;

/**
 * Service to retrieve {@link AccountSeed}. This class can be used by external classes of datacloud/match. For
 * internal usage, see {@link EntityMatchInternalService}
 */
public interface AccountEntityService {

    /**
     * Retrieve {@link AccountSeed} with the given ID under the target tenant.
     *
     * @param tenant target tenant
     * @param accountIds target ID
     * @return seed object, {@literal null} if no seed with the specified ID exists
     */
    AccountSeed get(@NotNull Tenant tenant, @NotNull String accountIds);

    /**
     *
     *
     * @param tenant target tenant
     * @param accountIds input list of IDs
     * @return a list of seed objects. the list will not be {@literal null} and will have the same size as the input
     * list of IDs. If no seed with a specific ID exists, {@literal null} will be inserted in the respective index.
     */
    List<AccountSeed> get(@NotNull Tenant tenant, @NotNull List<String> accountIds);
}
