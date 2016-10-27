package com.latticeengines.datacloud.etl.transformation.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;

public interface TransformationProgressUpdater {

    TransformationProgressUpdater status(String status);

    TransformationProgressUpdater retry();

    TransformationProgressUpdater applicationId(ApplicationId applicationId);

    TransformationProgressUpdater progress(Float progress);

    TransformationProgress commit();

}
