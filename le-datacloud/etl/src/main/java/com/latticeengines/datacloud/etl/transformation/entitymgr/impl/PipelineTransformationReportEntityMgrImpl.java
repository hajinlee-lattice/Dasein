package com.latticeengines.datacloud.etl.transformation.entitymgr.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.etl.transformation.dao.PipelineTransformationReportDao;
import com.latticeengines.datacloud.etl.transformation.entitymgr.PipelineTransformationReportEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.PipelineTransformationReportByStep;

@Component("pipelineTransformationReportEntityMgr")
public class PipelineTransformationReportEntityMgrImpl implements PipelineTransformationReportEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(PipelineTransformationReportEntityMgrImpl.class);

    @Autowired
    private PipelineTransformationReportDao reportDao;

    @Override
    @Transactional(value = "propDataManage")
    public void insertReportByStep(PipelineTransformationReportByStep stepReport) {
        try {
            reportDao.create(stepReport);
        } catch (Exception e) {
            log.error("Failed to create pipeline transfomration report", e);
        }
    }

    @Override
    @Transactional(value = "propDataManage")
    public void deleteReport(String pipeline, String version) {
        try {
            reportDao.deleteReport(pipeline, version);
        } catch (Exception e) {
            log.error("Failed to create pipeline transfomration report", e);
        }
    }
}
