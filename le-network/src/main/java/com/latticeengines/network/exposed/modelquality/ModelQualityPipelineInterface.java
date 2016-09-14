package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import org.springframework.http.HttpEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;

public interface ModelQualityPipelineInterface {

    List<Pipeline> getPipelines();
    
    Pipeline createPipelineFromProduction();

    String uploadPipelineStepPythonScript(String fileName, String stepName, MultipartFile file);
    
    String uploadPipelineStepPythonScript(String fileName, String stepName, HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity);
    
    String uploadPipelineStepMetadata(String fileName, String stepName, HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity);
    
    String uploadPipelineStepMetadata(String fileName, String stepName, MultipartFile file);

    String createPipeline(String pipelineName, List<PipelineStepOrFile> pipelineSteps);

    Pipeline getPipelineByName(String pipelineName);

}
