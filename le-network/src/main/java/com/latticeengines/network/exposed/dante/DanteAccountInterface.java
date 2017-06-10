package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DanteAccount;

public interface DanteAccountInterface {

    ResponseDocument<List<DanteAccount>> getAccounts(int count);
}
