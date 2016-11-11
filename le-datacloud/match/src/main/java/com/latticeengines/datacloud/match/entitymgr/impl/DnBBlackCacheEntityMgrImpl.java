package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.dnb.DnBBlackCache;
import com.latticeengines.datacloud.match.entitymgr.DnBBlackCacheEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

public class DnBBlackCacheEntityMgrImpl extends BaseFabricEntityMgrImpl<DnBBlackCache>
        implements DnBBlackCacheEntityMgr {
    public DnBBlackCacheEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService,
            String version) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
                .recordType("DnBBlackCache" + version).topic("DnBBlackCacheLookup") //
                .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("DataCloud"));
    }
}
