package com.latticeengines.datafabric.entitymanager.impl;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

class TestDynamoEntityMgrImpl extends BaseFabricEntityMgrImpl<TestDynamoEntity> {

    static final String RECORD_TYPE = "testRecord";

    TestDynamoEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService, String repo) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
                .recordType(RECORD_TYPE) //
                .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository(repo));
    }

}
