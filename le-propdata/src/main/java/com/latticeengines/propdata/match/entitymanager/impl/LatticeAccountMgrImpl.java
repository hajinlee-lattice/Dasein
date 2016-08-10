package com.latticeengines.propdata.match.entitymanager.impl;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;

import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.propdata.match.LatticeAccount;
import com.latticeengines.propdata.match.entitymanager.LatticeAccountMgr;

public class LatticeAccountMgrImpl extends BaseFabricEntityMgrImpl<LatticeAccount>
                                   implements LatticeAccountMgr  {
    public LatticeAccountMgrImpl(FabricMessageService messageService, FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("LatticeAccount" + version).topic("LatticeAccount") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("DataCloud"));
    }
}
