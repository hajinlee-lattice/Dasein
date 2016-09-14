package com.latticeengines.modelquality.controller;

import java.util.List;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource for analytic pipelines")
@RestController
@RequestMapping("/analyticpipelines")
public class AnalyticPipelineResource implements CrudInterface<AnalyticPipeline> {

    
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create analytic pipeline")
    public Boolean createAnalyticPipeline(@RequestBody AnalyticPipeline analyticPipeline) {
        return true;
    }
    
    
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create analytic pipeline for production")
    public AnalyticPipeline createAnalyticPipelineFromProduction() {
        return null;
    }


    @Override
    public AnalyticPipeline createForProduction() {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public AnalyticPipeline getByName(String name) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public List<AnalyticPipeline> getAll() {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public String create(AnalyticPipeline config, Object... params) {
        // TODO Auto-generated method stub
        return null;
    }
}
