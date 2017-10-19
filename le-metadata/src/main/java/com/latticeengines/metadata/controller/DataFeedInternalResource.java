package com.latticeengines.metadata.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.metadata.service.DataFeedService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeed_internal", description = "REST resource for metadata data feed internal operations")
@RestController
@RequestMapping("/datafeed/internal")
public class DataFeedInternalResource {

    @Autowired
    private DataFeedService dataFeedService;

    @RequestMapping(value = "/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "get all data feeds.")
    public List<DataFeed> getAllDataFeeds() {
        return dataFeedService.getAllDataFeeds();
    }

}
