package com.latticeengines.modelquality.controller;

import java.util.List;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "modelquality", description = "REST resource for analytic tests")
@RestController
@RequestMapping("/analytictests")
public class AnalyticTestResource implements CrudInterface<AnalyticTest> {

    
    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Create analytic test")
    public ResponseDocument<String> createAnalyticTest(@RequestBody AnalyticTest analyticTest) {
        // This should return a unique analytic id that can be referenced in the run call
        return null;
    }
    
    
    @RequestMapping(value = "/{analyticTestId}", method = RequestMethod.PUT)
    @ResponseBody
    @ApiOperation(value = "Execute analytic test")
    public ResponseDocument<String> runAnalyticTest(@PathVariable String analyticTestId) {
        return null;
    }


    @Override
    public AnalyticTest createForProduction() {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public AnalyticTest getByName(String name) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public List<AnalyticTest> getAll() {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public String create(AnalyticTest config, Object... params) {
        // TODO Auto-generated method stub
        return null;
    }

}
