package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteAccount;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
public interface DanteAccountService {
    List<DanteAccount> getAccounts(int count, String customerSpace);
}
