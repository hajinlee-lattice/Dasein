package com.latticeengines.ulysses.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.ulysses.LatticeInsights;
import com.latticeengines.ulysses.entitymgr.LatticeInsightsEntityMgr;

@Component("latticeInsightsEntityMgr")
public class LatticeInsightsEntityMgrImpl extends BaseFabricEntityMgrImpl<LatticeInsights> implements
        LatticeInsightsEntityMgr {

    @Autowired
    public LatticeInsightsEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
                .recordType("LatticeInsights").topic("LatticeInsights") //
                .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("Ulysses"));
    }
}
