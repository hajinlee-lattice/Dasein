package com.latticeengines.propdata.engine.ingestion.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;

public interface IngestionProgressDao extends BaseDao<IngestionProgress> {

    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields);

    public IngestionProgress saveProgress(IngestionProgress progress);

    public void deleteProgressByField(Map<String, Object> fields);

    public boolean isIngestionTriggered(Ingestion ingestion);

    public List<IngestionProgress> getRetryFailedProgresses();

    public boolean isDuplicateProgress(IngestionProgress progress);
}
