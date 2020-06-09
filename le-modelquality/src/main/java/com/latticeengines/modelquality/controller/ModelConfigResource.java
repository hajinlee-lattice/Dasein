package com.latticeengines.modelquality.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.modelquality.entitymgr.ModelConfigEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource for Model Config")
@RestController
public class ModelConfigResource {

    @Inject
    private ModelConfigEntityMgr modelConfigEntityMgr;

    private static final Logger log = LoggerFactory.getLogger(ModelConfigResource.class);

    @GetMapping("/modelconfigs")
    @ResponseBody
    @ApiOperation(value = "Get ModelConfigs")
    public ResponseDocument<List<ModelConfig>> getModelConfigs() {
        try {
            List<ModelConfig> modelConfigs = modelConfigEntityMgr.findAll();
            return ResponseDocument.successResponse(modelConfigs);

        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping("/modelconfigs")
    @ResponseBody
    @ApiOperation(value = "Upsert ModelConfigs")
    public ResponseDocument<String> upsertModelConfigs(@RequestBody List<ModelConfig> modelconfigs) {
        try {
            modelConfigEntityMgr.deleteAll();
            modelConfigEntityMgr.createModelConfigs(modelconfigs);
            return ResponseDocument.successResponse("OK");
        } catch (Exception e) {
            log.error("Failed on this API!", e);
            return ResponseDocument.successResponse("OK");
        }
    }
}
