package com.latticeengines.datacloud.etl.ingestion.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;

public interface IngestionEntityMgr {
    Ingestion getIngestionByName(String ingestionName);

    List<Ingestion> findAll();
}
