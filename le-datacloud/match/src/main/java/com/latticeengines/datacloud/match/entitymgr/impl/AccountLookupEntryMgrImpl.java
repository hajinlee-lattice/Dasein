package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;

public class AccountLookupEntryMgrImpl extends BaseFabricEntityMgrImpl<AccountLookupEntry>
        implements AccountLookupEntryMgr {    public AccountLookupEntryMgrImpl(FabricDataService dataService,
                                                                               String version) {
    super(new BaseFabricEntityMgrImpl.Builder() //
            .dataService(dataService) //
            .recordType("AccountLookup" + version) //
            .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
            .enforceRemoteDynamo(true) //
            .repository("DataCloud"));
}

}
