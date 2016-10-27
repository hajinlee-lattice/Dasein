package com.latticeengines.datacloud.etl.ingestion.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

public interface IngestionDao extends BaseDao<Ingestion> {
    Ingestion getIngestionByName(String name);
}
