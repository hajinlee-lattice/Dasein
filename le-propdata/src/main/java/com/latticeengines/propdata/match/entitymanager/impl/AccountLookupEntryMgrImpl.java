package com.latticeengines.propdata.match.entitymanager.impl;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;

import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.propdata.match.AccountLookupEntry;
import com.latticeengines.propdata.match.entitymanager.AccountLookupEntryMgr;

public class AccountLookupEntryMgrImpl extends BaseFabricEntityMgrImpl<AccountLookupEntry>
                                   implements AccountLookupEntryMgr  {
    public AccountLookupEntryMgrImpl(FabricMessageService messageService, FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("AccountLookup" + version).topic("LatticeAccountLookup") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("DataCloud"));
    }
}
