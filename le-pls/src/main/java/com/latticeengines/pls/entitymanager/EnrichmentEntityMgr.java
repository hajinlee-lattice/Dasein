package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Enrichment;

public interface EnrichmentEntityMgr extends BaseEntityMgr<Enrichment> {

    Enrichment createEnrichment();

    void deleteEnrichmentById(String enrichmentId);

}
