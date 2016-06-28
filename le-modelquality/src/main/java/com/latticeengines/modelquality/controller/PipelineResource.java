package com.latticeengines.modelquality.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineStepEntityMgr;

@Api(value = "modelquality", description = "REST resource to get Pipeline parameters")
@RestController
public class PipelineResource {

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    @Autowired
    private PipelineStepEntityMgr pipelineStepEntityMgr;

    private static final Log log = LogFactory.getLog(PipelineResource.class);

    @RequestMapping(value = "/pipelines", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Pipelines")
    public ResponseDocument<List<Pipeline>> getPipelines() {
        try {
            List<Pipeline> pipeLines = pipelineEntityMgr.findAll();
            return ResponseDocument.successResponse(pipeLines);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/pipelines", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Upsert Pipelines")
    public ResponseDocument<String> upsertPipelines(@RequestBody List<Pipeline> pipelines) {

        try {
            List<Pipeline> oldPipelines = pipelineEntityMgr.findAll();
            pipelineEntityMgr.deletePipelines(oldPipelines);
            pipelineEntityMgr.createPipelines(pipelines);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

}
