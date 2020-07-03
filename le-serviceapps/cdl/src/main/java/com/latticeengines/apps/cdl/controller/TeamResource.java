package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.domain.exposed.auth.TeamEntities;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "export-field-metadata", description = "Rest resource for export field metadata")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/teams")
public class TeamResource {

    @Inject
    private PlayService playService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private SegmentService segmentService;

    @GetMapping("/team-entities")
    @ResponseBody
    @ApiOperation(value = "get all entities that can append to team")
    public TeamEntities getTeamEntities(@PathVariable String customerSpace) {
        TeamEntities teamEntities = new TeamEntities();
        teamEntities.setMetadataSegments(segmentService.getSegments());
        teamEntities.setPlays(playService.getAllPlays());
        teamEntities.setRatingEngineSummaries(ratingEngineService.getRatingEngineSummaries());
        return teamEntities;
    }

}
