package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteAccount;

public interface DanteAccountInterface {

    List<DanteAccount> getAccounts(int count, String customerSpace);
}
