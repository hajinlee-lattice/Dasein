package com.latticeengines.datafabric.entitymanager.impl;

import com.latticeengines.datafabric.entitymanager.GenericTableActivityMgr;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;

public class GenericTableActivityMgrImpl extends BaseFabricEntityMgrImpl<GenericTableActivity> implements GenericTableActivityMgr {
    public GenericTableActivityMgrImpl(FabricDataService dataService,
                                       String signature) {
        super(new BaseFabricEntityMgrImpl.Builder()
                .dataService(dataService)
                .recordType(String.format("%s_%s", GenericTableActivity.class.getSimpleName(), signature))
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO)
                .enforceRemoteDynamo(true)
                .repository("GenericTable")
        );
    }
}
