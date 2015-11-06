package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.pls.service.ProspectDiscoveryConfigurationService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "prospectDiscoveryConfiguration", description = "REST resource for prospect discovery configurations")
@RestController
@RequestMapping("/prospectdiscoveryconfiguration")
@PreAuthorize("hasRole('View_PLS_Configurations')")
public class ProspectDiscoveryConfigurationResource {
    
    @Autowired
    ProspectDiscoveryConfigurationService prospectDiscoveryConfigurationService;

    @RequestMapping(value = "/{option}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public String findValueForOption(@PathVariable String option) {
        return this.prospectDiscoveryConfigurationService.findProspectDiscoveryOption(option).getValue();
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public ProspectDiscoveryConfiguration findProspectDiscoveryConfiguration() {
        List<ProspectDiscoveryOption> prospectDiscoveryOptions = this.prospectDiscoveryConfigurationService.findAllProspectDiscoveryOptions();
        return new ProspectDiscoveryConfiguration(prospectDiscoveryOptions);
    }

    @RequestMapping(value = "/{option}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ApiOperation(value = "Update value for prospect discovery option")
    @PreAuthorize("hasRole('Edit_PLS_Configurations')")
    public void update(@PathVariable String option, @RequestBody String value) {
        this.prospectDiscoveryConfigurationService.updateValueForOption(option, value);
    }

}
