package com.latticeengines.dante.dao;

import java.util.List;

import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.domain.exposed.dante.DanteAccount;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
public interface DanteAccountDao extends BaseDanteDao<DanteAccount> {

    List<DanteAccount> getAccounts(int count, String customerID);
}
