package com.latticeengines.objectapi.controller;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.objectapi.service.EventQueryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "events", description = "REST resource for modeling events")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/event")
public class EventResource {
    private final EventQueryService eventQueryService;

    @Autowired
    public EventResource(EventQueryService eventQueryService) {
        this.eventQueryService = eventQueryService;
    }

    @RequestMapping(value = "/count/scoring", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getScoringCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
                                @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getScoringCount(frontEndQuery, version);
    }

    @RequestMapping(value = "/count/training", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getTrainingCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
                                 @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getTrainingCount(frontEndQuery, version);
    }

    @RequestMapping(value = "/count/event", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    public long getEventCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
                              @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getEventCount(frontEndQuery, version);
    }

    @RequestMapping(value = "/data/scoring", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getScoringTuples(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
                                     @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getScoringTuples(frontEndQuery, version);
    }

    @RequestMapping(value = "/data/training", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getTrainingTuples(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
                                      @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getTrainingTuples(frontEndQuery, version);
    }

    @RequestMapping(value = "/data/event", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getEventTuples(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
                                   @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getEventTuples(frontEndQuery, version);
    }

}
