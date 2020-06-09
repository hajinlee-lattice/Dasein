package com.latticeengines.modelquality.controller;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.network.exposed.modelquality.ModelQualityModelRunInterface;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource to run model for model quality")
@RestController
@RequestMapping("/modelruns")
public class ModelRunResource implements ModelQualityModelRunInterface, CrudInterface<ModelRunEntityNames> {

    @Inject
    private ModelRunService modelRunService;

    @Inject
    private ModelRunEntityMgr modelRunEntityMgr;

    @Inject
    private ModelProxy modelProxy;

    @Override
    @GetMapping("/")
    @ResponseBody
    @ApiOperation(value = "Get ModelRuns")
    public List<ModelRunEntityNames> getModelRuns() {
        return getAll();
    }

    @Override
    @PostMapping("/")
    @ResponseBody
    @ApiOperation(value = "Create model run")
    public String createModelRun(@RequestBody ModelRunEntityNames modelRunEntityNames, //
            @RequestParam("tenant") String tenant, //
            @RequestParam("username") String username, //
            @RequestParam("password") String password, //
            @RequestParam("apiHostPort") String apiHostPort) {
        return create(modelRunEntityNames, tenant, username, password, apiHostPort);
    }

    @Override
    @GetMapping("/{modelRunName:.*}")
    @ResponseBody
    @ApiOperation(value = "Get ModelRun by name")
    public ModelRunEntityNames getModelRunByName(@PathVariable String modelRunName) {
        return getByName(modelRunName);
    }

    @Override
    @GetMapping("/status/{modelRunName:.*}")
    @ResponseBody
    @ApiOperation(value = "Get ModelRun status by name")
    public String getModelRunStatusByName(@PathVariable String modelRunName) {
        return getStatusByName(modelRunName).toString();
    }

    @Override
    @GetMapping("/modelhdfsdir/{modelRunName:.*}")
    @ResponseBody
    @ApiOperation(value = "Get ModelRun model HDFS directory by name")
    public String getModelRunModelHDFSDirByName(@PathVariable String modelRunName) {
        return getModelHDFSDirByName(modelRunName);
    }

    @Override
    public ModelRunEntityNames getByName(String name) {
        ModelRun run = modelRunEntityMgr.findByName(name);
        ModelRunEntityNames runnames = new ModelRunEntityNames(run);
        return runnames;
    }

    public ModelRunStatus getStatusByName(String name) {
        ModelRun run = modelRunEntityMgr.findByName(name);
        return run.getStatus();
    }

    public String getModelHDFSDirByName(String name) {
        ModelRun run = modelRunEntityMgr.findByName(name);
        String modelId = run.getModelId();
        Model model = modelProxy.getModel(UuidUtils.extractUuid(modelId));
        return model.getModelHdfsDir();
    }

    @Override
    public String create(ModelRunEntityNames config, Object... params) {
        Environment env = new Environment();
        env.tenant = (String) params[0];
        env.username = (String) params[1];
        env.password = (String) params[2];
        env.apiHostPort = (String) params[3];
        modelRunService.setEnvironment(env);
        ModelRun run = modelRunService.createModelRun(config, env);
        return run.getName();
    }

    @Override
    public List<ModelRunEntityNames> getAll() {
        List<ModelRunEntityNames> result = new ArrayList<>();
        for (ModelRun run : modelRunEntityMgr.findAll()) {
            ModelRunEntityNames runnames = new ModelRunEntityNames(run);
            result.add(runnames);
        }
        return result;
    }
}
