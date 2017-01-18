package com.latticeengines.ulysses.service.impl;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.ulysses.entitymgr.EnrichedAccountEntityMgr;
import com.latticeengines.ulysses.entitymgr.impl.EnrichedAccountEntityMgrImpl;
import com.latticeengines.ulysses.service.EnrichedAccountService;

@Component("enrichedAccountService")
public class EnrichedAccountServiceImpl implements EnrichedAccountService {

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    private EnrichedAccountEntityMgr scoreAndEnrichmentEntityMgr;

    public EnrichedAccountServiceImpl() {
    }

    @PostConstruct
    public void postConstruct() throws Exception {
        scoreAndEnrichmentEntityMgr = new EnrichedAccountEntityMgrImpl(messageService, dataService);
        scoreAndEnrichmentEntityMgr.init();
    }

}
