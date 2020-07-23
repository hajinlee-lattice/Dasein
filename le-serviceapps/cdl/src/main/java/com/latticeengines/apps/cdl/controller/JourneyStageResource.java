package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.JourneyStageService;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "journeyStages", description = "REST resource for JourneyStage management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/journeyStages")
public class JourneyStageResource {

    @Inject
    private JourneyStageService journeyStageService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation("Get all journeyStages under current tenant")
    public List<JourneyStage> getJourneyStages(@PathVariable(value = "customerSpace") String customerSpace) {
        return journeyStageService.findByTenant(customerSpace);
    }

    @PostMapping("")
    @ResponseBody
    @ApiOperation("Create or update a journeyStage under current tenant")
    public JourneyStage createJourneyStage( //
                                    @PathVariable(value = "customerSpace") String customerSpace, //
                                    @RequestBody JourneyStage journeyStage) {
        return journeyStageService.createOrUpdate(customerSpace, journeyStage);
    }

    @PostMapping("/createDefault")
    @ResponseBody
    @ApiOperation("create default JourneyStage under current tenant.")
    public Boolean createDefaultJourneyStage(@PathVariable(value = "customerSpace") String customerSpace) {
        journeyStageService.createDefaultJourneyStages(customerSpace);
        return true;
    }
}
