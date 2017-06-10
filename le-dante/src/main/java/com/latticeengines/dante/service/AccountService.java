package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteAccount;

public interface AccountService {
    List<DanteAccount> getAccounts(int count);
}
