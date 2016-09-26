package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;

public interface ModelQualityAnalyticPipelineInterface {

    List<AnalyticPipelineEntityNames> getAnalyticPipelines();

    AnalyticPipelineEntityNames createAnalyticPipelineFromProduction();

    String createAnalyticPipeline(AnalyticPipelineEntityNames analyticPipelineNames);

    AnalyticPipelineEntityNames getAnalyticPipelineByName(String analyticPipelineName);

}
