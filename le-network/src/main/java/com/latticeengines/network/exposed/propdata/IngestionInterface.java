package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;

public interface IngestionInterface {
    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod);

    public List<IngestionProgress> scan(String hdfsPod);
}
