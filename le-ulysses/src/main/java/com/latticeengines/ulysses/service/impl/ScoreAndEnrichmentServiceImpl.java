package com.latticeengines.ulysses.service.impl;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.ulysses.entitymgr.ScoreAndEnrichmentEntityMgr;
import com.latticeengines.ulysses.entitymgr.impl.ScoreAndEnrichmentEntityMgrImpl;
import com.latticeengines.ulysses.service.ScoreAndEnrichmentService;

@Component("scoreAndEnrichmentService")
public class ScoreAndEnrichmentServiceImpl implements ScoreAndEnrichmentService {

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;
    
    private ScoreAndEnrichmentEntityMgr scoreAndEnrichmentEntityMgr;
    
    public ScoreAndEnrichmentServiceImpl() {
    }

    @PostConstruct
    public void postConstruct() throws Exception {
        scoreAndEnrichmentEntityMgr = new ScoreAndEnrichmentEntityMgrImpl(messageService, dataService);
        scoreAndEnrichmentEntityMgr.init();
    }

}
