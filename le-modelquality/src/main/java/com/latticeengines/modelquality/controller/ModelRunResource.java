package com.latticeengines.modelquality.controller;

import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.service.ModelRunService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to run model for model quality")
@RestController
public class ModelRunResource {

    @Resource(name = "modelRunService")
    private ModelRunService modelRunService;

    @Autowired
    private ModelRunEntityMgr modelRunEntityMgr;

    private static final Log log = LogFactory.getLog(ModelRunResource.class);

    @RequestMapping(value = "/runmodel", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Run a Model")
    public ResponseDocument<String> runModel(@RequestBody ModelRun modelRun, //
            @RequestParam("tenant") String tenant, //
            @RequestParam("username") String username, //
            @RequestParam("password") String encryptedPassword, //
            @RequestParam("apiHostPort") String apiHostPort) {
        try {
            Environment env = new Environment();
            env.tenant = tenant;
            env.username = username;
            env.encryptedPassword = encryptedPassword;
            env.apiHostPort = apiHostPort;
            modelRunService.setEnvironment(env);
            String modelRunId = modelRunService.run(modelRun, env);
            return ResponseDocument.successResponse(modelRunId);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/modelruns", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ModelRuns")
    public ResponseDocument<List<ModelRun>> getModelRuns() {
        try {
            List<ModelRun> modelRuns = modelRunEntityMgr.findAll();
            return ResponseDocument.successResponse(modelRuns);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/modelrun/{modelRunId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get ModelRun")
    public ResponseDocument<ModelRun> getModelRun(@PathVariable String modelRunId) {
        try {
            ModelRun modelRun = new ModelRun();
            modelRun.setPid(Long.valueOf(modelRunId));
            ModelRun returnedModelRun = modelRunEntityMgr.findByKey(modelRun);
            return ResponseDocument.successResponse(returnedModelRun);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/modelruns", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete ModelRuns")
    public void deleteModelRuns() {
        try {
            modelRunEntityMgr.deleteAll();
        } catch (Exception e) {
            log.error("Failed on this API!", e);
        }
    }
}
