package com.latticeengines.modelquality.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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

    @Autowired
    private AnalyticTestService analyticTestService;

    @Override
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get AnalyticTest")
    public List<AnalyticTestEntityNames> getAnalyticTests() {
        return getAll();
    }

    @Override
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create analytic test")
    public String createAnalyticTest(@RequestBody AnalyticTestEntityNames analyticTestEntityNames) {
        return create(analyticTestEntityNames);
    }

    @Override
    @RequestMapping(value = "/{analyticTestName:.*}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get AnalyticTest by name")
    public AnalyticTestEntityNames getAnalyticTestByName(@PathVariable String analyticTestName) {
        return getByName(analyticTestName);
    }

    @Override
    @RequestMapping(value = "/execute/{analyticTestName:.*}", method = RequestMethod.PUT)
    @ResponseBody
    @ApiOperation(value = "Execute AnalyticTest by name")
    public List<ModelRun> executeAnalyticTestByName(@PathVariable String analyticTestName) {
        return analyticTestService.executeByName(analyticTestName);
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
