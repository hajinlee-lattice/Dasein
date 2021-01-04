package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;

public class LatticeAccountMgrImpl extends BaseFabricEntityMgrImpl<LatticeAccount> implements LatticeAccountMgr {
    public LatticeAccountMgrImpl(FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder() //
                .dataService(dataService) //
                .recordType("LatticeAccount" + version) //
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                .enforceRemoteDynamo(true) //
                .repository("DataCloud"));
    }
}
