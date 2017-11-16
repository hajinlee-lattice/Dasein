package com.latticeengines.objectapi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
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

    @RequestMapping(value = "/data/scoring", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getScoringTuples(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery) {
        return eventQueryService.getScoringTuples(frontEndQuery);
    }

    @RequestMapping(value = "/data/training", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getTrainingTuples(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery) {
        return eventQueryService.getTrainingTuples(frontEndQuery);
    }

    @RequestMapping(value = "/data/event", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Retrieve the rows for the specified query")
    public DataPage getEventTuples(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery) {
        return eventQueryService.getEventTuples(frontEndQuery);
    }

}
