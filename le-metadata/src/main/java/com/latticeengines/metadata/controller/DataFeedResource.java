package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.metadata.service.DataFeedService;
import com.latticeengines.network.exposed.metadata.DataFeedInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "REST resource for metadata data feeds")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datafeeds")
public class DataFeedResource implements DataFeedInterface {

    @Autowired
    private DataFeedService datafeedService;

    @RequestMapping(value = "/{datafeedName}/startexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "start data feed execution")
    @Override
    public DataFeedExecution startExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName) {
        return datafeedService.startExecution(customerSpace, datafeedName);
    }

    @RequestMapping(value = "/{datafeedName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    @Override
    public DataFeed findDataFeedByName(@PathVariable String customerSpace, @PathVariable String datafeedName) {
        return datafeedService.findDataFeedByName(customerSpace, datafeedName);
    }

    @RequestMapping(value = "/{datafeedName}/finishexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "finish data feed execution")
    @Override
    public DataFeedExecution finishExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName) {
        return datafeedService.finishExecution(customerSpace, datafeedName);
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "create data feed")
    @Override
    public DataFeed createDataFeed(@PathVariable String customerSpace, @RequestBody DataFeed datafeed) {
        return datafeedService.createDataFeed(customerSpace, datafeed);
    }
}
