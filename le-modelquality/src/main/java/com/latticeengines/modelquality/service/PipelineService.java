package com.latticeengines.modelquality.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;

public interface PipelineService {

    Pipeline createPipeline(String pipelineName, List<PipelineStepOrFile> pipelineSteps);

    Pipeline createLatestProductionPipeline();

    String uploadPipelineStepFile(String stepName, InputStream inputStream, String extension, boolean isMetadata);

}
