package com.latticeengines.ulysses.entitymgr.impl;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.ulysses.EnrichedAccount;
import com.latticeengines.ulysses.entitymgr.EnrichedAccountEntityMgr;

public class EnrichedAccountEntityMgrImpl extends BaseFabricEntityMgrImpl<EnrichedAccount>
    implements EnrichedAccountEntityMgr {

    public EnrichedAccountEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("EnrichedAccount").topic("EnrichedAccount") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("Ulysses"));
    }

}
