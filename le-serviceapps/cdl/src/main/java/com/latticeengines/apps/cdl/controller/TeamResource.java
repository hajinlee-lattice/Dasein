package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.domain.exposed.auth.TeamEntityList;
import com.latticeengines.security.exposed.service.TeamService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Team Management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/teams")
public class TeamResource {

    @Inject
    private PlayService playService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private SegmentService segmentService;

    @Inject
    private TeamService teamService;

    @GetMapping("/team-entities")
    @ResponseBody
    @ApiOperation(value = "get all entities that can assign team")
    public TeamEntityList getTeamEntities(@PathVariable String customerSpace) {
        TeamEntityList teamEntityList = new TeamEntityList();
        teamEntityList.setMetadataSegments(segmentService.getSegments());
        teamEntityList.setPlays(playService.getAllPlays());
        teamEntityList.setRatingEngineSummaries(ratingEngineService.getRatingEngineSummaries());
        return teamEntityList;
    }

    @PostMapping("/default")
    @ResponseBody
    @ApiOperation(value = "create default global team")
    public String createDefaultTeam(@PathVariable String customerSpace) {
        return teamService.createDefaultTeam();
    }

}
