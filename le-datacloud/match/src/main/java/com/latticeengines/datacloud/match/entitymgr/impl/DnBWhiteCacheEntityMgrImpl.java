package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.dnb.DnBWhiteCache;
import com.latticeengines.datacloud.match.entitymgr.DnBWhiteCacheEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

public class DnBWhiteCacheEntityMgrImpl extends BaseFabricEntityMgrImpl<DnBWhiteCache>
        implements DnBWhiteCacheEntityMgr {
    public DnBWhiteCacheEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService,
            String version) {
        super(new BaseFabricEntityMgrImpl.Builder() //
                .messageService(messageService) //
                .dataService(dataService) //
                .recordType("DnBWhiteCache" + version) //
                .topic("DnBWhiteCacheLookup") //
                .scope(TopicScope.ENVIRONMENT_PRIVATE) //
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                .enforceRemoteDynamo(true) //
                .repository("DataCloud"));
    }
}
