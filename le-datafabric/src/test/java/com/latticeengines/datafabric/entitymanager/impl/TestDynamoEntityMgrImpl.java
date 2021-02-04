package com.latticeengines.datafabric.entitymanager.impl;

import com.latticeengines.datafabric.service.datastore.FabricDataService;

class TestDynamoEntityMgrImpl extends BaseFabricEntityMgrImpl<TestDynamoEntity> {

    static final String RECORD_TYPE = "testRecord";

    TestDynamoEntityMgrImpl(FabricDataService dataService, String repo) {
        super(new BaseFabricEntityMgrImpl.Builder().dataService(dataService) //
                .recordType(RECORD_TYPE) //
                .enforceRemoteDynamo(true) //
                .store("DYNAMO").repository(repo));
    }

}
