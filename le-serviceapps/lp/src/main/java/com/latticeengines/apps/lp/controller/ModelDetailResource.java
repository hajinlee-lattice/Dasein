package com.latticeengines.apps.lp.controller;


import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.lp.service.ModelDetailService;
import com.latticeengines.domain.exposed.pls.ModelDetail;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "model detail", description = "REST resource for model detail")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/modeldetail")
public class ModelDetailResource {

    @Inject
    private ModelDetailService modelDetailService;

    @PostMapping("/modelid/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Get model detail")
    public ModelDetail getModelDetail(@PathVariable String customerSpace, @PathVariable String modelId) {
        return modelDetailService.getModelDetail(modelId);
    }

}
