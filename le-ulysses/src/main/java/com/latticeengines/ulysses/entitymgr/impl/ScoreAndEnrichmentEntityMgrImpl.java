package com.latticeengines.ulysses.entitymgr.impl;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.ulysses.ScoreAndEnrichmentRecord;
import com.latticeengines.ulysses.entitymgr.ScoreAndEnrichmentEntityMgr;

public class ScoreAndEnrichmentEntityMgrImpl extends BaseFabricEntityMgrImpl<ScoreAndEnrichmentRecord>
    implements ScoreAndEnrichmentEntityMgr {

    public ScoreAndEnrichmentEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("ScoreAndEnrichmentRecord").topic("ScoreAndEnrichment") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("Ulysses"));
    }

}
