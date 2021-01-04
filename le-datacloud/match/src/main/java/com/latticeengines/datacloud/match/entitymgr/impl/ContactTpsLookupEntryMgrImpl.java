package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.entitymgr.ContactTpsLookupEntryMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.match.ContactTpsLookupEntry;

public class ContactTpsLookupEntryMgrImpl extends BaseFabricEntityMgrImpl<ContactTpsLookupEntry>
        implements ContactTpsLookupEntryMgr {
    public ContactTpsLookupEntryMgrImpl(FabricDataService dataService,
                                     String version) {
        super(new BaseFabricEntityMgrImpl.Builder() //
                .dataService(dataService) //
                .recordType(ContactMasterConstants.CONTACT_TPS_LOOKUPENTRY_RECORD_TYPE + version) //
                .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                .enforceRemoteDynamo(true) //
                .repository("DataCloud"));
    }
}
