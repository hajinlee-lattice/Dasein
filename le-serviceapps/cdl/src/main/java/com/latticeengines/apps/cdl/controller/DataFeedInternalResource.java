package com.latticeengines.apps.cdl.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;

@Api(value = "datafeed_internal", description = "REST resource for metadata data feed internal operations")
@RestController
@RequestMapping("/datafeed/internal")
public class DataFeedInternalResource {

    @Autowired
    private DataFeedService dataFeedService;

    @RequestMapping(value = "/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "get all data feeds.")
    public List<DataFeed> getAllDataFeeds() {
        return dataFeedService.getAllDataFeeds();
    }

    @RequestMapping(value = "/simpledatafeedlist", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "get all simple data feeds.")
    public List<SimpleDataFeed> getAllSimpleDataFeeds() {
        return dataFeedService.getAllSimpleDataFeeds();
    }

    @RequestMapping(value = "/simpledatafeedlistforactivetenant", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @NoCustomerSpace
    @ApiOperation(value = "get all simple data feeds.")
    public List<SimpleDataFeed> getAllSimpleDataFeedsForActiveTenant() {
        return dataFeedService.getAllSimpleDataFeedsForActiveTenant();
    }

}
