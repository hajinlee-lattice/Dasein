package com.latticeengines.propdata.engine.ingestion.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.propdata.engine.ingestion.service.impl.IngestionProgressServiceImpl.IngestionProgressUpdaterImpl;

public interface IngestionProgressService {
    public List<IngestionProgress> getProgressesByField(Map<String, Object> fields);

    public IngestionProgress createStagingIngestionProgress(Ingestion ingestion, String triggeredBy,
            String file);

    public String constructSource(Ingestion ingestion, String fileName);

    public String constructDestination(Ingestion ingestion, String fileName);

    public void saveProgresses(List<IngestionProgress> progresses);

    public List<IngestionProgress> getNewIngestionProgresses();

    public List<IngestionProgress> getRetryFailedProgresses();

    public CamelRouteConfiguration createCamelRouteConfiguration(IngestionProgress progress);

    public IngestionProgress saveProgress(IngestionProgress progress);

    public IngestionProgressUpdaterImpl updateProgress(IngestionProgress progress);

    public IngestionProgress updateSubmittedProgress(IngestionProgress progress,
            String applicationId);

    public IngestionProgress updateDuplicateProgress(IngestionProgress progress);

    public void deleteProgress(IngestionProgress progress);

    public void deleteProgressByField(Map<String, Object> fields);

}
