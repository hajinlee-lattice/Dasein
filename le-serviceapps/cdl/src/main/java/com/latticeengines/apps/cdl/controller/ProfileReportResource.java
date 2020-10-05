package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ProfileReportService;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Profile Report")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/profile-report")
public class ProfileReportResource {

    @Inject
    private ProfileReportService profileReportService;

    @GetMapping("/status")
    @ResponseBody
    @ApiOperation(value = "Get status of profile report.")
    public AtlasProfileReportStatus getProfileReportStatus(@PathVariable String customerSpace) {
        return profileReportService.getStatus();
    }

}
