package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.AggregationHistoryService;
import com.latticeengines.domain.exposed.cdl.AggregationHistory;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "aggregationhistory", description = "REST resource for aggregation history")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/aggregation-history")
public class AggregationHistoryResource {

    private static final Logger log = LoggerFactory.getLogger(AggregationHistoryResource.class);

    @Inject
    private AggregationHistoryService aggregationHistoryService;

    @PostMapping("")
    @ApiOperation(value = "create aggregation history")
    public AggregationHistory create(@PathVariable String customerSpace, @RequestBody AggregationHistory aggregationHistory) {
        return aggregationHistoryService.create(aggregationHistory);
    }
}
