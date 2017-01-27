package com.latticeengines.datacloud.core.entitymgr.impl;

import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

public class DnBCacheEntityMgrImpl extends BaseFabricEntityMgrImpl<DnBCache> implements DnBCacheEntityMgr {
    public DnBCacheEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder() //
                .messageService(messageService) //
                .dataService(dataService) //
                .recordType("DnBCache" + version) //
                .topic("DnBCacheLookup") //
                .scope(TopicScope.ENVIRONMENT_PRIVATE) //
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                .enforceRemoteDynamo(true) //
                .repository("DataCloud"));
    }
}
