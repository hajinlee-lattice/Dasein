package com.latticeengines.apps.lp.controller;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.lp.service.ModelCleanUpService;
import com.latticeengines.apps.lp.service.ModelReplaceService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.serviceapps.lp.ReplaceModelRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "model operation", description = "HTTP controller for cross-tenant model operations")
@RestController
@RequestMapping("/modeloperation")
public class ModelOperationController {

    @Inject
    private ModelCleanUpService modelCleanUpService;

    @Inject
    private ModelReplaceService modelReplaceService;

    @DeleteMapping("/cleanup/modelid/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Cleanup a model")
    @NoCustomerSpace
    public SimpleBooleanResponse cleanupModel(@PathVariable String modelId) {
        Boolean success = modelCleanUpService.cleanUpModel(modelId);
        if (Boolean.TRUE.equals(success)) {
            return SimpleBooleanResponse.successResponse();
        } else {
            return SimpleBooleanResponse
                    .failedResponse(Collections.singletonList("Failed to cleanup model " + modelId));
        }
    }

    @PostMapping("/replace")
    @ResponseBody
    @ApiOperation(value = "Replace a model")
    @NoCustomerSpace
    public SimpleBooleanResponse replaceModel(@RequestBody ReplaceModelRequest request) {
        Boolean success = modelReplaceService.replaceModel(request);
        if (Boolean.TRUE.equals(success)) {
            return SimpleBooleanResponse.successResponse();
        } else {
            return SimpleBooleanResponse.failedResponse(
                    Collections.singletonList("Failed to replace model with request " + JsonUtils.serialize(request)));
        }
    }

}
