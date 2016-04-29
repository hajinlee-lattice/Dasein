package com.latticeengines.propdata.engine.ingestion.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;

public interface IngestionDao extends BaseDao<Ingestion> {
    public Ingestion getIngestionByName(String name);
}
