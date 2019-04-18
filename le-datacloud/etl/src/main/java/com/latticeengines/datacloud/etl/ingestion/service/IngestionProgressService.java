package com.latticeengines.datacloud.etl.ingestion.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.datacloud.etl.ingestion.service.impl.IngestionProgressServiceImpl;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;

public interface IngestionProgressService {
    List<IngestionProgress> getProgressesByField(Map<String, Object> fields, List<String> orderFields);

    IngestionProgress createDraftProgress(Ingestion ingestion, String triggeredBy, String file, String version);

    void saveProgresses(List<IngestionProgress> progresses);

    List<IngestionProgress> getNewIngestionProgresses();

    List<IngestionProgress> getRetryFailedProgresses();

    List<IngestionProgress> getProcessingProgresses();

    CamelRouteConfiguration createCamelRouteConfiguration(IngestionProgress progress);

    IngestionProgress saveProgress(IngestionProgress progress);

    IngestionProgressServiceImpl.IngestionProgressUpdaterImpl updateProgress(IngestionProgress progress);

    IngestionProgress updateSubmittedProgress(IngestionProgress progress,
                                                     String applicationId);

    IngestionProgress updateInvalidProgress(IngestionProgress progress, String message);

    void deleteProgress(IngestionProgress progress);

    void deleteProgressByField(Map<String, Object> fields);

}
