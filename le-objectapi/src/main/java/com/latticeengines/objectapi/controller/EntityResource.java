package com.latticeengines.objectapi.controller;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.network.exposed.objectapi.EntityInterface;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluator;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "entities", description = "REST resource for entities")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class EntityResource implements EntityInterface {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    protected QueryEvaluator queryEvaluator;

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(EntityResource.class);

    @Override
    @RequestMapping(value = "/entities/count", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getCount(@PathVariable String customerSpace, @RequestBody Query query) {
        long count = -1;
        try (PerformanceTimer timer = new PerformanceTimer("fetch count")) {
            count = queryEvaluator.evaluate(dataCollectionProxy.getDefaultAttributeRepository(customerSpace), query)
                    .fetchCount();
        }
        return count;
    }

    @Override
    @RequestMapping(value = "/entities/data", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getData(@PathVariable String customerSpace, @RequestBody Query query) {
        DataPage dataPage = null;
        try (PerformanceTimer timer = new PerformanceTimer("fetch data")) {
            List<Map<String, Object>> results = queryEvaluator
                    .run(dataCollectionProxy.getDefaultAttributeRepository(customerSpace), query).getData();
            dataPage = new DataPage(results);
        }

        return dataPage;
    }

}
