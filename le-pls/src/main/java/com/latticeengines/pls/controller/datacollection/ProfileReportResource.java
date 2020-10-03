package com.latticeengines.pls.controller.datacollection;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;
import com.latticeengines.pls.service.ProfileReportService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Profile Report")
@RestController
@RequestMapping("/profile-reports")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class ProfileReportResource {

    @Inject
    private ProfileReportService profileReportService;

    @PostMapping("/refresh")
    @ResponseBody
    @ApiOperation(value = "Refresh profile report.")
    @PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
    public AtlasProfileReportStatus refreshProfile() {
        return profileReportService.refreshProfile();
    }

    @GetMapping("/status")
    @ResponseBody
    @ApiOperation(value = "Get profile report status.")
    public AtlasProfileReportStatus getStatus() {
        return profileReportService.getStatus();
    }

}
