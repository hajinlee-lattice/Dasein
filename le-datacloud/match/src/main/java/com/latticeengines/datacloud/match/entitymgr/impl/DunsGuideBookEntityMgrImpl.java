package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.entitymgr.DunsGuideBookEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;

/**
 * Entity manager for {@link DunsGuideBook}, entity is persisted in Dynamo table.
 * Go to {@link BaseFabricEntityMgrImpl} for details.
 */
public class DunsGuideBookEntityMgrImpl extends BaseFabricEntityMgrImpl<DunsGuideBook> implements DunsGuideBookEntityMgr {
    private static final String RECORD_TYPE_PREFIX = DunsGuideBook.class.getSimpleName();
    private static final String REPOSITORY = "DataCloud";

    public DunsGuideBookEntityMgrImpl(FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder() //
                .dataService(dataService) //
                .recordType(RECORD_TYPE_PREFIX + version) //
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                .enforceRemoteDynamo(true) //
                .repository(REPOSITORY));
    }
}
