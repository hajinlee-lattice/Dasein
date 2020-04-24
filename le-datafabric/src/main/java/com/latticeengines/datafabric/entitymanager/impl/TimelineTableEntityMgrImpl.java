package com.latticeengines.datafabric.entitymanager.impl;

import com.latticeengines.datafabric.entitymanager.TimelineTableEntityMgr;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TimelineTableEntity;

public class TimelineTableEntityMgrImpl extends BaseFabricEntityMgrImpl<TimelineTableEntity> implements TimelineTableEntityMgr {
    public TimelineTableEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService,
                                      String signature) {
        super(new BaseFabricEntityMgrImpl.Builder()
                .messageService(messageService)
                .dataService(dataService)
                .recordType(String.format("%s_%s", TimelineTableEntity.class.getSimpleName(), signature))
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO)
                .enforceRemoteDynamo(true)
                .repository("TimelineTable")
        );
    }
}
