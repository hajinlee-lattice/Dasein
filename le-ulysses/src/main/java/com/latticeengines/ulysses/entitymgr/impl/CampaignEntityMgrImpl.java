package com.latticeengines.ulysses.entitymgr.impl;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.ulysses.entitymgr.CampaignEntityMgr;

public class CampaignEntityMgrImpl extends BaseFabricEntityMgrImpl<Campaign>
    implements CampaignEntityMgr {

    public CampaignEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("Campaign").topic("Campaign") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("Ulysses"));
    }

}
