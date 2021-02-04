package com.latticeengines.ulysses.service.impl;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.ulysses.entitymgr.EnrichedAccountEntityMgr;
import com.latticeengines.ulysses.entitymgr.impl.EnrichedAccountEntityMgrImpl;
import com.latticeengines.ulysses.service.EnrichedAccountService;

@Component("enrichedAccountService")
public class EnrichedAccountServiceImpl implements EnrichedAccountService {

    @Inject
    private FabricDataService dataService;

    private EnrichedAccountEntityMgr scoreAndEnrichmentEntityMgr;

    public EnrichedAccountServiceImpl() {
    }

    @PostConstruct
    public void postConstruct() throws Exception {
        scoreAndEnrichmentEntityMgr = new EnrichedAccountEntityMgrImpl(dataService);
        scoreAndEnrichmentEntityMgr.init();
    }

}
