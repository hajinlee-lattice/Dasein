package com.latticeengines.dante.dao;

import java.util.List;

import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.domain.exposed.dante.DanteAccount;

public interface DanteAccountDao extends BaseDanteDao<DanteAccount> {

    List<DanteAccount> getAccounts(int count, String customerID);
}
