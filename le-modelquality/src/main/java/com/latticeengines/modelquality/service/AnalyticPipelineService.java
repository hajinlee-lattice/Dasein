package com.latticeengines.modelquality.service;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;

public interface AnalyticPipelineService {

    AnalyticPipeline createAnalyticPipeline(AnalyticPipelineEntityNames analyticPipelineEntityNames);

    AnalyticPipeline createLatestProductionAnalyticPipeline();
}
