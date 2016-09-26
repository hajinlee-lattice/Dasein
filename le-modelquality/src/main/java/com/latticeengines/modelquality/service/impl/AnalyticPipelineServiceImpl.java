package com.latticeengines.modelquality.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.service.AnalyticPipelineService;

@Component("analyticPipelineService")
public class AnalyticPipelineServiceImpl extends BaseServiceImpl implements AnalyticPipelineService {

    @Autowired
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Override
    public AnalyticPipeline createAnalyticPipeline(AnalyticPipelineEntityNames analyticPipelineEntityNames) {
        AnalyticPipeline analyticPipeline = new AnalyticPipeline();
        // NOTE: THIS IS A DUMMY IMPLEMENTATION THAT NEEDS TO BE COMPLETED
        return analyticPipeline;

    }

    @Override
    public AnalyticPipeline createLatestProductionAnalyticPipeline() {
        String version = getVersion();

        AnalyticPipeline analyticPipeline = new AnalyticPipeline();
        // NOTE: THIS IS A DUMMY IMPLEMENTATION THAT NEEDS TO BE COMPLETED
        return analyticPipeline;
    }

}
