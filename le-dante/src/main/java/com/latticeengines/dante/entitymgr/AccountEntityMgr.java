package com.latticeengines.dante.entitymgr;

import java.util.List;

import com.latticeengines.dantedb.exposed.entitymgr.BaseDanteEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteAccount;

public interface AccountEntityMgr extends BaseDanteEntityMgr<DanteAccount> {
    List<DanteAccount> getAccounts(int count);
}
