package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdldenpendencies", description = "REST resource for get CDL depending object")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/dependencies")
public class CDLDependenciesResource {

    @Inject
    private SegmentService segmentService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private PlayService playService;

    @RequestMapping(value = "/segments", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get impacted segments")
    public List<MetadataSegment> getDependingSegments(@PathVariable String customerSpace,
            @RequestBody List<String> attributes) {
        return segmentService.findDependingSegments(attributes);
    }

    @RequestMapping(value = "/ratingengines", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get impacted rating engines")
    public List<RatingEngine> getDependingRatingEngines(@PathVariable String customerSpace,
            @RequestBody List<String> attributes) {
        return ratingEngineService.getDependingRatingEngines(attributes);
    }

    @RequestMapping(value = "/ratingmodels", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get impacted rating models")
    public List<RatingModel> getDependingRatingModels(@PathVariable String customerSpace,
            @RequestBody List<String> attributes) {
        return ratingEngineService.getDependingRatingModels(attributes);
    }

    @RequestMapping(value = "/plays", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find Plays using the given attributes")
    public List<Play> findDependantPlays(@PathVariable String customerSpace, @RequestBody List<String> attributes) {
        return playService.findDependantPlays(attributes);
    }

}
