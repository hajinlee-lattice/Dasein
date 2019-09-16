package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.exposed.service.AccountEntityService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.entity.AccountSeed;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("accountEntityService")
public class AccountEntityServiceImpl implements AccountEntityService {

    private static final String ENTITY = AccountSeed.ENTITY;

    private final EntityMatchInternalService entityMatchInternalService;

    @Inject
    public AccountEntityServiceImpl(@NotNull EntityMatchInternalService entityMatchInternalService) {
        this.entityMatchInternalService = entityMatchInternalService;
    }

    @Override
    public AccountSeed get(@NotNull Tenant tenant, @NotNull String accountId) {
        check(tenant);
        Preconditions.checkNotNull(accountId);
        return toAccountSeed(entityMatchInternalService.get(tenant, ENTITY, accountId, null));
    }

    @Override
    public List<AccountSeed> get(@NotNull Tenant tenant, @NotNull List<String> accountIds) {
        check(tenant);
        Preconditions.checkNotNull(accountIds);
        return entityMatchInternalService.get(tenant, ENTITY, accountIds, null)
                .stream()
                .map(this::toAccountSeed)
                .collect(Collectors.toList());
    }

    private AccountSeed toAccountSeed(EntityRawSeed rawSeed) {
        if (rawSeed == null) {
            return null;
        }
        return AccountSeed.fromRawSeed(rawSeed);
    }

    private void check(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getId());
    }
}
