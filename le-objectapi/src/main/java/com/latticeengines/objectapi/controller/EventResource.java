package com.latticeengines.objectapi.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.monitor.exposed.annotation.InvocationMeter;
import com.latticeengines.objectapi.service.EventQueryService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "events", description = "REST resource for modeling events")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/event")
public class EventResource {
    private final EventQueryService eventQueryService;

    @Inject
    public EventResource(EventQueryService eventQueryService) {
        this.eventQueryService = eventQueryService;
    }

    @PostMapping(value = "/count/scoring")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name ="scoring", measurment = "objectapi")
    public Long getScoringCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getScoringCount(frontEndQuery, version);
    }

    @PostMapping(value = "/count/training")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name ="training", measurment = "objectapi")
    public Long getTrainingCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getTrainingCount(frontEndQuery, version);
    }

    @PostMapping(value = "/count/event")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name ="event", measurment = "objectapi")
    public Long getEventCount(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getEventCount(frontEndQuery, version);
    }

    @PostMapping(value = "/data/scoring")
    @ResponseBody
    @ApiOperation(value = "Retrieve the scoring tuples for the specified query")
    public DataPage getScoringTuples(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getScoringTuples(frontEndQuery, version);
    }

    @PostMapping(value = "/data/training")
    @ResponseBody
    @ApiOperation(value = "Retrieve the training tuples for the specified query")
    public DataPage getTrainingTuples(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getTrainingTuples(frontEndQuery, version);
    }

    @PostMapping(value = "/data/event")
    @ResponseBody
    @ApiOperation(value = "Retrieve the event tuples for the specified query")
    public DataPage getEventTuples(@PathVariable String customerSpace, @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getEventTuples(frontEndQuery, version);
    }

    @PostMapping(value = "/query")
    @ResponseBody
    @ApiOperation(value = "Retrieve the SQL for the specified query")
    public String getQuery(@PathVariable String customerSpace, //
            @RequestBody EventFrontEndQuery frontEndQuery, //
            @RequestParam(value = "eventType", required = true) EventType eventType, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getQueryStr(frontEndQuery, eventType, version);
    }

}
