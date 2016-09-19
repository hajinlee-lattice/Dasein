package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Enrichment;

public interface EnrichmentDao extends BaseDao<Enrichment> {

    void deleteEnrichmentById(String enrichmentId);

}
