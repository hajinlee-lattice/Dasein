package com.latticeengines.datafabric.entitymanager.impl;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;

class TestDynamoEntityMgrImpl extends BaseFabricEntityMgrImpl<TestDynamoEntity> {

    static final String RECORD_TYPE = "testRecord";

    TestDynamoEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService, String repo) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
                .recordType(RECORD_TYPE) //
                .store("DYNAMO").repository(repo));
    }

}
