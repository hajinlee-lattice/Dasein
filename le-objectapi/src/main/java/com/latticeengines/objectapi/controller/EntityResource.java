package com.latticeengines.objectapi.controller;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.network.exposed.objectapi.EntityInterface;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for entities")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class EntityResource implements EntityInterface {

    @Autowired
    private QueryEvaluatorService queryEvaluatorService;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(EntityResource.class);

    @Override
    @RequestMapping(value = "/entities/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@PathVariable String customerSpace, @RequestBody Query query) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return queryEvaluatorService.getCount(customerSpace, query);
    }

    @Override
    @RequestMapping(value = "/entities/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody Query query) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return queryEvaluatorService.getData(customerSpace, query);
    }

}
