package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.entitymgr.ContactTpsEntryMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.match.ContactTpsEntry;

public class ContactTpsEntryMgrImpl extends BaseFabricEntityMgrImpl<ContactTpsEntry> implements ContactTpsEntryMgr {

    public ContactTpsEntryMgrImpl(FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder() //
                .dataService(dataService) //
                .recordType(ContactMasterConstants.CONTACT_TPS_ENTRY_RECORD_TYPE + version) //
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                .enforceRemoteDynamo(true) //
                .repository("DataCloud"));
    }
}
