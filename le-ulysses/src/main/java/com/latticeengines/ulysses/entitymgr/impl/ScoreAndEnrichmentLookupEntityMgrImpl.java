package com.latticeengines.ulysses.entitymgr.impl;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.ulysses.ScoreAndEnrichmentLookupRecord;
import com.latticeengines.ulysses.entitymgr.ScoreAndEnrichmentLookupEntityMgr;

public class ScoreAndEnrichmentLookupEntityMgrImpl extends BaseFabricEntityMgrImpl<ScoreAndEnrichmentLookupRecord>
    implements ScoreAndEnrichmentLookupEntityMgr {

    public ScoreAndEnrichmentLookupEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
              .recordType("ScoreAndEnrichmentLookupRecord").topic("ScoreAndEnrichmentLookup") //
              .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("Ulysses"));
    }

}
