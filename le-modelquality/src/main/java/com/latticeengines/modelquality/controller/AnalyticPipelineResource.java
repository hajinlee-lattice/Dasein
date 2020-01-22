package com.latticeengines.modelquality.controller;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipelineEntityNames;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.service.AnalyticPipelineService;
import com.latticeengines.network.exposed.modelquality.ModelQualityAnalyticPipelineInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource for analytic pipelines")
@RestController
@RequestMapping("/analyticpipelines")
public class AnalyticPipelineResource
        implements ModelQualityAnalyticPipelineInterface, CrudInterface<AnalyticPipelineEntityNames> {

    @Inject
    private AnalyticPipelineService analyticPipelineService;

    @Inject
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get AnalyticPipelines")
    public List<AnalyticPipelineEntityNames> getAnalyticPipelines() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/latest", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create analytic pipeline for production")
    public AnalyticPipelineEntityNames createAnalyticPipelineFromProduction() {
        AnalyticPipeline ap = analyticPipelineService.createLatestProductionAnalyticPipeline();
        AnalyticPipelineEntityNames apnames = new AnalyticPipelineEntityNames(ap);
        return apnames;
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create analytic pipeline")
    public String createAnalyticPipeline(@RequestBody AnalyticPipelineEntityNames analyticPipelineEntityNames) {
        return create(analyticPipelineEntityNames);
    }

    @Override
    @RequestMapping(value = "/{analyticPipelineName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get AnalyticPipeline by name")
    public AnalyticPipelineEntityNames getAnalyticPipelineByName(@PathVariable String analyticPipelineName) {
        return getByName(analyticPipelineName);
    }

    @Override
    public AnalyticPipelineEntityNames getByName(String name) {
        AnalyticPipeline ap = analyticPipelineEntityMgr.findByName(name);
        AnalyticPipelineEntityNames apnames = new AnalyticPipelineEntityNames(ap);
        return apnames;
    }

    @Override
    public String create(AnalyticPipelineEntityNames config, Object... params) {
        AnalyticPipeline ap = analyticPipelineService.createAnalyticPipeline(config);
        return ap.getName();
    }

    @Override
    public List<AnalyticPipelineEntityNames> getAll() {
        List<AnalyticPipelineEntityNames> result = new ArrayList<>();
        for (AnalyticPipeline ap : analyticPipelineEntityMgr.findAll()) {
            AnalyticPipelineEntityNames apnames = new AnalyticPipelineEntityNames(ap);
            result.add(apnames);
        }
        return result;
    }
}
