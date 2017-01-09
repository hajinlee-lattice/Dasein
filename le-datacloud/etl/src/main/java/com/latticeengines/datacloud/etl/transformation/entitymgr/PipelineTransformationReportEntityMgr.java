package com.latticeengines.datacloud.etl.transformation.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;

public interface PipelineTransformationReportEntityMgr {
    void insertReportByStep(PipelineTransformationReportByStep stepReport);
    void deleteReport(String pipeline, String version);
}
