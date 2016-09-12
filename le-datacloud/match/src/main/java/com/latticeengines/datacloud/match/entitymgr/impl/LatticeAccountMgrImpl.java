package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;

public class LatticeAccountMgrImpl extends BaseFabricEntityMgrImpl<LatticeAccount>
                                   implements LatticeAccountMgr {
    public LatticeAccountMgrImpl(FabricMessageService messageService, FabricDataService dataService, String version) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("LatticeAccount" + version).topic("LatticeAccount") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("DataCloud"));
    }
}
