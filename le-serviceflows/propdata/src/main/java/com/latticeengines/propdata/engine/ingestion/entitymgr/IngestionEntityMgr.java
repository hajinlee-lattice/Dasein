package com.latticeengines.propdata.engine.ingestion.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

public interface IngestionEntityMgr {
    public Ingestion getIngestionByName(String ingestionName);

    public List<Ingestion> findAll();
}
