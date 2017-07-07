package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteAccount;

public interface DanteAccountService {
    List<DanteAccount> getAccounts(int count, String customerSpace);
}
