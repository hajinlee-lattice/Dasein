package com.latticeengines.datacloud.etl.transformation.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;

public interface PipelineTransformationReportDao extends BaseDao<PipelineTransformationReportByStep> {

    void deleteReport(String pipeline, String version);
}
