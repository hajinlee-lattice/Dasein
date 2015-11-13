package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.pls.service.QuotaService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "quota", description = "REST resource for quotas")
@RestController
@RequestMapping("/quotas")
@PreAuthorize("hasRole('View_PLS_Quotas')")
public class QuotaResource {

    @Autowired
    private QuotaService quotaService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a quota object")
    @PreAuthorize("hasRole('Create_PLS_Quotas')")
    public void create(@RequestBody Quota quota) {
        this.quotaService.create(quota);
    }

    @RequestMapping(value = "/{quotaId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a quota object")
    @PreAuthorize("hasRole('Edit_PLS_Quotas')")
    public void delete(@PathVariable String quotaId) {
        this.quotaService.deleteQuotaByQuotaId(quotaId);
    }

    @RequestMapping(value = "/{quotaId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "View a quota object")
    @PreAuthorize("hasRole('View_PLS_Quotas')")
    public Quota find(@PathVariable String quotaId) {
        return this.quotaService.findQuotaByQuotaId(quotaId);
    }

    @RequestMapping(value = "/{quotaId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a quota object")
    @PreAuthorize("hasRole('Edit_PLS_Quotas')")
    public void update(@PathVariable String quotaId, @RequestBody Quota quota) {
        this.quotaService.updateQuotaByQuotaId(quota, quotaId);
    }

}
