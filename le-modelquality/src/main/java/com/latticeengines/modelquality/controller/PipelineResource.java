package com.latticeengines.modelquality.controller;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.service.PipelineService;
import com.latticeengines.modelquality.service.impl.PipelineStepType;
import com.latticeengines.network.exposed.modelquality.ModelQualityPipelineInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to get Pipeline parameters")
@RestController
@RequestMapping("/pipelines")
public class PipelineResource implements ModelQualityPipelineInterface, CrudInterface<Pipeline> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineResource.class);

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    @Autowired
    private PipelineService pipelineService;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get the list of pipeline instances")
    public List<Pipeline> getPipelines() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = " the latest active production pipeline")
    public Pipeline createPipelineFromProduction() {
        return createForProduction();
    }

    @Override
    @RequestMapping(method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create new pipeline from a list of either existing pipeline steps or uploaded files")
    public String createPipeline(@RequestParam("pipelineName") String pipelineName,
            @RequestParam(value = "pipelineDescription", required = false) String pipelineDescription,
            @RequestBody List<PipelineStepOrFile> pipelineSteps) {
        return create(null, pipelineName, pipelineDescription, pipelineSteps);
    }

    @Override
    @RequestMapping(value = "/{pipelineName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get pipeline by name")
    public Pipeline getPipelineByName(@PathVariable String pipelineName) {
        return getByName(pipelineName);
    }

    @Override
    @RequestMapping(value = "/pipelinestepfiles/metadata", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload custom python pipeline metadata file")
    public String uploadPipelineStepMetadata(@RequestParam(value = "fileName", required = true) String fileName, //
            @RequestParam(value = "stepName", required = true) String stepName, //
            @RequestParam("file") MultipartFile file) {
        try {
            return pipelineService.uploadPipelineStepFile(stepName, file.getInputStream(), //
                    new String[] { stepName, "json" }, PipelineStepType.METADATA);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @RequestMapping(value = "/pipelinestepfiles/python", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload custom python pipeline implementation file")
    public String uploadPipelineStepPythonScript(@RequestParam(value = "fileName", required = true) String fileName, //
            @RequestParam(value = "stepName", required = true) String stepName, //
            @RequestParam("file") MultipartFile file) {
        try {
            return pipelineService.uploadPipelineStepFile(stepName, file.getInputStream(), //
                    new String[] { stepName, "py" }, PipelineStepType.PYTHONLEARNING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @RequestMapping(value = "/pipelinestepfiles/pythonrts", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upload custom python RTS pipeline implementation file")
    public String uploadPipelineStepRTSPythonScript(@RequestParam(value = "fileName", required = true) String fileName, //
            @RequestParam(value = "stepName", required = true) String stepName, //
            @RequestParam("file") MultipartFile file) {
        try {
            String moduleName = fileName.split("\\.")[0];
            return pipelineService.uploadPipelineStepFile(stepName, file.getInputStream(), //
                    new String[] { moduleName, "py" }, PipelineStepType.PYTHONRTS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Methods below are just to satisfy the interface but not exposed as part
    // of the REST API

    @Override
    public String uploadPipelineStepPythonScript(String fileName, String stepName,
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        return null;
    }

    @Override
    public String uploadPipelineStepMetadata(String fileName, String stepName,
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        return null;
    }

    @Override
    public String uploadPipelineStepRTSPythonScript(String fileName, String stepName,
            HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity) {
        return null;
    }

    public Pipeline createForProduction() {
        return pipelineService.createLatestProductionPipeline();
    }

    @Override
    public Pipeline getByName(String name) {
        return pipelineEntityMgr.findByName(name);
    }

    @Override
    public List<Pipeline> getAll() {
        return pipelineEntityMgr.findAll();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public String create(Pipeline config, Object... params) {
        String pipelineName = (String) params[0];
        String pipelineDescription = (String) params[1];
        List<PipelineStepOrFile> pipelineSteps = (List) params[2];
        Pipeline p = pipelineService.createPipeline(pipelineName, pipelineDescription, pipelineSteps);
        return p.getName();
    }

}
