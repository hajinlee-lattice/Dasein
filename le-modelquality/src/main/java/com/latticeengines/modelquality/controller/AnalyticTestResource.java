package com.latticeengines.modelquality.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.service.AnalyticTestService;
import com.latticeengines.network.exposed.modelquality.ModelQualityAnalyticTestInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource for analytic tests")
@RestController
@RequestMapping("/analytictests")
public class AnalyticTestResource implements ModelQualityAnalyticTestInterface, CrudInterface<AnalyticTestEntityNames> {

    @Inject
    private AnalyticTestService analyticTestService;

    @Override
    @GetMapping("/")
    @ResponseBody
    @ApiOperation(value = "Get AnalyticTest")
    public List<AnalyticTestEntityNames> getAnalyticTests() {
        return getAll();
    }

    @Override
    @PostMapping("/")
    @ResponseBody
    @ApiOperation(value = "Create analytic test")
    public String createAnalyticTest(@RequestBody AnalyticTestEntityNames analyticTestEntityNames) {
        return create(analyticTestEntityNames);
    }

    @Override
    @GetMapping("/{analyticTestName:.*}")
    @ResponseBody
    @ApiOperation(value = "Get AnalyticTest by name")
    public AnalyticTestEntityNames getAnalyticTestByName(@PathVariable String analyticTestName) {
        return getByName(analyticTestName);
    }

    @Override
    @PutMapping("/execute/{analyticTestName:.*}")
    @ResponseBody
    @ApiOperation(value = "Execute AnalyticTest by name")
    public List<ModelRun> executeAnalyticTestByName(@PathVariable String analyticTestName) {
        return analyticTestService.executeByName(analyticTestName);
    }

    @Override
    @PutMapping("/updateproduction")
    @ResponseBody
    @ApiOperation(value = "Update the production analytic pipeline in existing analytic tests")
    public List<AnalyticTest> updateProductionAnalyticPipeline() {
        return analyticTestService.updateProductionAnalyticPipeline();
    }

    @Override
    public AnalyticTestEntityNames getByName(String name) {
        return analyticTestService.getByName(name);
    }

    @Override
    public String create(AnalyticTestEntityNames config, Object... params) {
        AnalyticTest atest = analyticTestService.createAnalyticTest(config);
        return atest.getName();
    }

    @Override
    public List<AnalyticTestEntityNames> getAll() {
        return analyticTestService.getAll();
    }
}
