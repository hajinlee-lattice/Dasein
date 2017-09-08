package com.latticeengines.ulysses.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.ulysses.EnrichedAccount;
import com.latticeengines.ulysses.entitymgr.EnrichedAccountEntityMgr;

@Component("enrichedAccountEntityMgr")
public class EnrichedAccountEntityMgrImpl extends BaseFabricEntityMgrImpl<EnrichedAccount> implements
        EnrichedAccountEntityMgr {

    @Autowired
    public EnrichedAccountEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
                .recordType("EnrichedAccount").store("DYNAMO").repository("Ulysses"));
    }

}
