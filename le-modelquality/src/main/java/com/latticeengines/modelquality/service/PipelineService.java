package com.latticeengines.modelquality.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.modelquality.service.impl.PipelineStepType;

public interface PipelineService {

    Pipeline createPipeline(String pipelineName, String pipelineDescription, List<PipelineStepOrFile> pipelineSteps);

    Pipeline createLatestProductionPipeline();

    String uploadPipelineStepFile(String stepName, InputStream inputStream, String[] names, PipelineStepType type);
}
