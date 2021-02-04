package com.latticeengines.ulysses.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.ulysses.EnrichedAccount;
import com.latticeengines.ulysses.entitymgr.EnrichedAccountEntityMgr;

@Component("enrichedAccountEntityMgr")
public class EnrichedAccountEntityMgrImpl extends BaseFabricEntityMgrImpl<EnrichedAccount> implements
        EnrichedAccountEntityMgr {

    @Inject
    public EnrichedAccountEntityMgrImpl(FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().dataService(dataService) //
                .recordType("EnrichedAccount").store("DYNAMO").repository("Ulysses"));
    }

}
