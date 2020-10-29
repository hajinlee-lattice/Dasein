package com.latticeengines.objectapi.controller;

import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_BATCH;

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

    @PostMapping("/count/scoring")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name = "scoring", measurment = "objectapi")
    public Long getScoringCount(@PathVariable String customerSpace, //
                                @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
                                @RequestBody EventFrontEndQuery frontEndQuery, //
                                @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getScoringCount(frontEndQuery, sqlUser, version);
    }

    @PostMapping("/count/training")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name = "training", measurment = "objectapi")
    public Long getTrainingCount(@PathVariable String customerSpace, //
                                 @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
                                 @RequestBody EventFrontEndQuery frontEndQuery,
                                 @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getTrainingCount(frontEndQuery, sqlUser, version);
    }

    @PostMapping("/count/event")
    @ResponseBody
    @ApiOperation(value = "Retrieve the number of rows for the specified query")
    @InvocationMeter(name = "event", measurment = "objectapi")
    public Long getEventCount(@PathVariable String customerSpace, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
            @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getEventCount(frontEndQuery, sqlUser, version);
    }

    @PostMapping("/data/scoring")
    @ResponseBody
    @ApiOperation(value = "Retrieve the scoring tuples for the specified query")
    public DataPage getScoringTuples(@PathVariable String customerSpace, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
            @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getScoringTuples(frontEndQuery, sqlUser, version);
    }

    @PostMapping("/data/training")
    @ResponseBody
    @ApiOperation(value = "Retrieve the training tuples for the specified query")
    public DataPage getTrainingTuples(@PathVariable String customerSpace, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
            @RequestBody EventFrontEndQuery frontEndQuery,
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getTrainingTuples(frontEndQuery, sqlUser, version);
    }

    @PostMapping("/data/event")
    @ResponseBody
    @ApiOperation(value = "Retrieve the event tuples for the specified query")
    public DataPage getEventTuples(@PathVariable String customerSpace, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
            @RequestBody EventFrontEndQuery frontEndQuery, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getEventTuples(frontEndQuery, sqlUser, version);
    }

    @PostMapping("/query")
    @ResponseBody
    @ApiOperation(value = "Retrieve the SQL for the specified query")
    public String getQuery(@PathVariable String customerSpace, //
            @RequestParam(value = "sqlUser", required = false, defaultValue = USER_BATCH) String sqlUser, //
            @RequestBody EventFrontEndQuery frontEndQuery, //
            @RequestParam(value = "eventType", required = true) EventType eventType, //
            @RequestParam(value = "version", required = false) DataCollection.Version version) {
        return eventQueryService.getQueryStr(frontEndQuery, eventType, sqlUser, version);
    }

}
