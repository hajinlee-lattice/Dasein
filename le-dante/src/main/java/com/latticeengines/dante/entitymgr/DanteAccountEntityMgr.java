package com.latticeengines.dante.entitymgr;

import java.util.List;

import com.latticeengines.dantedb.exposed.entitymgr.BaseDanteEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteAccount;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
public interface DanteAccountEntityMgr extends BaseDanteEntityMgr<DanteAccount> {
    List<DanteAccount> getAccounts(int count, String customerID);
}
